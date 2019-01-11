package com.provectus.fds.it;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.dynamodb.models.Aggregation;
import com.provectus.fds.it.aws.CloudFormation;
import com.provectus.fds.models.bcns.Bid;
import com.provectus.fds.models.bcns.ClickBcn;
import com.provectus.fds.models.bcns.ImpressionBcn;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AwsFdsTestIT {
    private static final String STACK_NAME_PREFIX = "fdsit";
    private static final String REGION = "us-west-2";
    private static CloudFormation cloudFormation;
    private static String reportUrl;
    private static String apiUrl;

    private final AsyncHttpClient httpClient = asyncHttpClient(config());
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void beforeClass() throws Exception {
        cloudFormation = new CloudFormation(REGION
                , String.format("%s_%s", STACK_NAME_PREFIX, UUID.randomUUID().toString())
                , new File("fds.yaml")
        );
        reportUrl = cloudFormation.getOutput("UrlForAPI").getOutputValue();
        apiUrl = cloudFormation.getOutput("UrlForReports").getOutputValue();
    }

    @After
    public void afterClass() throws Exception {
        if (cloudFormation!=null) cloudFormation.close();
    }

    @Test
    public void testStackOutputs() {
        assertNotNull(reportUrl);
        assertNotNull(apiUrl);
    }

    @Test
    public void testDynamoTotalReport() throws IOException, ExecutionException, InterruptedException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int length = 10;
        String domain = "www.google.com";
        String creativeCategory = "testCreativeCategory";
        String creativeId = UUID.randomUUID().toString();
        long campaignItemId = random.nextLong(1_000_000L, 2_000_000L);

        Map<String, List<ListenableFuture<Response>>> futuresByType = new HashMap<>();

        for (int i = 0; i < length; i++) {

            String txid = UUID.randomUUID().toString();
            String appuid = UUID.randomUUID().toString();
            long winPrice = random.nextInt(1_000, 2_000);

            Bid bid = new Bid(txid,campaignItemId, domain,creativeId,creativeCategory, appuid);
            ImpressionBcn impressionBcn = new ImpressionBcn(txid, Instant.now().toEpochMilli(),winPrice);
            ClickBcn clickBcn = new ClickBcn(txid, Instant.now().toEpochMilli());


            futuresByType.computeIfAbsent("bid", (k) -> new ArrayList<>())
                    .add(sendRequest("bid", bid));
            futuresByType.computeIfAbsent("impression", (k) -> new ArrayList<>())
                    .add(sendRequest("impression", impressionBcn));
            futuresByType.computeIfAbsent("click", (k) -> new ArrayList<>())
                    .add(sendRequest("click", clickBcn));
        }

        int countOfBids = awaitSuccessfull(futuresByType.get("bid"));
        int countOfImpressions = awaitSuccessfull(futuresByType.get("impression"));
        int countOfClicks = awaitSuccessfull(futuresByType.get("click"));

        awaitReport(campaignItemId, countOfBids, countOfImpressions, countOfClicks);
    }

    private void awaitReport(long campaignItemId, int countOfBids, int countOfImpressions, int countOfClicks) {
        await().atMost(15, TimeUnit.MINUTES)
                .until( () -> {
                    Aggregation report = getReport(campaignItemId);
                    assertEquals(countOfBids,report.getBids());
                    assertEquals(countOfImpressions, report.getImps());
                    assertEquals(countOfClicks, report.getClicks());
                    return true;
                });
    }

    private int awaitSuccessfull(List<ListenableFuture<Response>> futures) {
        int n = 0;
        for (ListenableFuture<Response> future : futures) {
            try {
                Response response = future.get();
                if (response.getStatusCode()==200) {
                    n++;
                }
            } catch (Throwable e) {

            }
        }
        return n;
    }

    private Aggregation getReport(long campaignItemId) throws IOException, ExecutionException, InterruptedException {
        Response response = httpClient.prepareGet(reportUrl +"/reports/campaigns/"+campaignItemId)
                .addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8")
                .execute()
                .get();

        return objectMapper.readValue(response.getResponseBodyAsBytes(), Aggregation.class);
    }

    private <T> ListenableFuture<Response> sendRequest(String type, T model) throws JsonProcessingException {
        return httpClient.preparePost(String.format("%s/%s", apiUrl, type))
                .addHeader("Content-Type", "application/json")
                .addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8")
                .setBody(objectMapper.writeValueAsBytes(model))
                .execute();
    }

}
