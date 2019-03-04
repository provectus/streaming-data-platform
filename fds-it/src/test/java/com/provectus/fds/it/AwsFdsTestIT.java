package com.provectus.fds.it;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.dynamodb.models.Aggregation;
import com.provectus.fds.it.aws.CloudFormation;
import com.provectus.fds.models.bcns.BidBcn;
import com.provectus.fds.models.bcns.ClickBcn;
import com.provectus.fds.models.bcns.ImpressionBcn;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.junit.*;

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
    private static final String STACK_NAME_PREFIX = "integration";
    private static final String REGION = "us-west-2";

    public static final String URL_FOR_API = "UrlForAPI";
    public static final String URL_FOR_REPORTS = "UrlForReports";
    public static final String BID_TYPE = "bid";
    public static final String IMP_TYPE = "impression";
    public static final String CLICK_TYPE = "click";
    public static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8";

    private static CloudFormation cloudFormation;
    private static String reportUrl;
    private static String apiUrl;

    private final AsyncHttpClient httpClient = asyncHttpClient(config());
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    public static void beforeClass() throws Exception {
        cloudFormation = new CloudFormation(REGION
                , String.format("%s%s", STACK_NAME_PREFIX, UUID.randomUUID().toString().replace("-","")).substring(0,30)
                , new File("fds.yaml")
        );
        reportUrl = cloudFormation.getOutput(URL_FOR_REPORTS).getOutputValue();
        apiUrl = cloudFormation.getOutput(URL_FOR_API).getOutputValue();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (cloudFormation != null) cloudFormation.close();
    }

    @Test
    public void testStackOutputs() {
        assertNotNull(reportUrl);
        assertNotNull(apiUrl);
    }

    @Test
    public void testDynamoTotalReport() throws IOException, ExecutionException, InterruptedException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numberOfBids = random.nextInt(100, 200);
        int numberOfImps = random.nextInt(numberOfBids / 4, numberOfBids / 2);
        int numberOfClicks = random.nextInt(numberOfImps / 4, numberOfImps / 2);

        String domain = "www.google.com";
        String creativeCategory = "testCreativeCategory";
        String creativeId = UUID.randomUUID().toString();
        long campaignItemId = random.nextLong(1_000_000L, 2_000_000L);

        Map<String, List<ListenableFuture<Response>>> futuresByType = new HashMap<>();

        for (int i = 0; i < numberOfBids; i++) {

            String txid = UUID.randomUUID().toString();
            String appuid = UUID.randomUUID().toString();
            long winPrice = random.nextInt(1_000, 2_000);

            BidBcn bidBcn = new BidBcn(txid, campaignItemId, domain, creativeId, creativeCategory, appuid);
            ImpressionBcn impressionBcn = new ImpressionBcn(txid, Instant.now().toEpochMilli(), winPrice);
            ClickBcn clickBcn = new ClickBcn(txid, Instant.now().toEpochMilli());


            futuresByType.computeIfAbsent(BID_TYPE, (k) -> new ArrayList<>())
                    .add(sendRequest(BID_TYPE, bidBcn));

            if (numberOfImps > 0) {
                futuresByType.computeIfAbsent(IMP_TYPE, (k) -> new ArrayList<>())
                        .add(sendRequest(IMP_TYPE, impressionBcn));
                numberOfImps--;
            }

            if (numberOfClicks > 0) {
                futuresByType.computeIfAbsent(CLICK_TYPE, (k) -> new ArrayList<>())
                        .add(sendRequest(CLICK_TYPE, clickBcn));
                numberOfClicks--;
            }
        }

        int countOfBids = awaitSuccessfull(futuresByType.get(BID_TYPE));
        int countOfImpressions = awaitSuccessfull(futuresByType.get(IMP_TYPE));
        int countOfClicks = awaitSuccessfull(futuresByType.get(CLICK_TYPE));

        await().atMost(15, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.SECONDS)
                .until(() -> {
                    Aggregation report = getReport(campaignItemId);
                    return countOfBids == report.getBids() &&
                            countOfImpressions == report.getImps() &&
                            countOfClicks == report.getClicks();
                });
    }

    private int awaitSuccessfull(List<ListenableFuture<Response>> futures) {
        int n = 0;
        for (ListenableFuture<Response> future : futures) {
            try {
                Response response = future.get();
                if (response.getStatusCode() == 200) {
                    n++;
                }
            } catch (Throwable e) {
                System.out.println("ERROR: " + e.getMessage());
            }
        }
        return n;
    }

    private Aggregation getReport(long campaignItemId) throws IOException, ExecutionException, InterruptedException {
        Response response = httpClient.prepareGet(reportUrl + "/reports/campaigns/" + campaignItemId)
                .addHeader("User-Agent", USER_AGENT)
                .execute()
                .get();
        return objectMapper.readValue(response.getResponseBodyAsBytes(), Aggregation.class);
    }

    private <T> ListenableFuture<Response> sendRequest(String type, T model) throws JsonProcessingException {
        return httpClient.preparePost(String.format("%s/%s", apiUrl, type))
                .addHeader("Content-Type", "application/json")
                .addHeader("User-Agent", USER_AGENT)
                .setBody(objectMapper.writeValueAsBytes(model))
                .execute();
    }

}
