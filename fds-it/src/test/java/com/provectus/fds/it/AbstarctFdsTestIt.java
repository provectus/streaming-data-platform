package com.provectus.fds.it;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.it.aws.CloudFormation;
import com.provectus.fds.models.bcns.Bid;
import com.provectus.fds.models.bcns.ClickBcn;
import com.provectus.fds.models.bcns.ImpressionBcn;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

abstract class AbstarctFdsTestIt implements ItConfig {

    static final String URL_FOR_API = "UrlForAPI";
    static final String URL_FOR_REPORTS = "UrlForReports";

    static CloudFormation cloudFormation;
    static String reportUrl;
    static String apiUrl;

    final AsyncHttpClient httpClient = asyncHttpClient(config());
    final ObjectMapper objectMapper = new ObjectMapper();


    <T> ListenableFuture<Response> sendRequest(String type, T model) throws JsonProcessingException {
        return httpClient.preparePost(String.format("%s/%s", apiUrl, type))
                .addHeader("Content-Type", "application/json")
                .addHeader("User-Agent", USER_AGENT)
                .setBody(objectMapper.writeValueAsBytes(model))
                .execute();
    }

    int awaitSuccessfull(List<ListenableFuture<Response>> futures) {
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

    SampleDataResult generateSampleData() throws JsonProcessingException {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numberOfBids = random.nextInt(100, 200);
        int numberOfImps = random.nextInt(numberOfBids / 4, numberOfBids / 2);
        int numberOfClicks = random.nextInt(numberOfImps / 4, numberOfImps / 2);

        String domain = "www.google.com";
        String creativeCategory = "testCreativeCategory";
        String creativeId = UUID.randomUUID().toString();
        long campaignItemId = random.nextLong(1_000_000L, 2_000_000L);
        SampleDataResult result = new SampleDataResult(campaignItemId);

        Map<String, List<ListenableFuture<Response>>> futuresByType = new HashMap<>();

        for (int i = 0; i < numberOfBids; i++) {

            String txid = UUID.randomUUID().toString();
            String appuid = UUID.randomUUID().toString();
            long winPrice = random.nextInt(1_000, 2_000);

            Bid bid = new Bid(txid, campaignItemId, domain, creativeId, creativeCategory, appuid);
            ImpressionBcn impressionBcn = new ImpressionBcn(txid, Instant.now().toEpochMilli(), winPrice);
            ClickBcn clickBcn = new ClickBcn(txid, Instant.now().toEpochMilli());


            futuresByType.computeIfAbsent(BID_TYPE, (k) -> new ArrayList<>())
                    .add(sendRequest(BID_TYPE, bid));

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

        result.setCountOfBids(awaitSuccessfull(futuresByType.get(BID_TYPE)));
        result.setCountOfImpressions(awaitSuccessfull(futuresByType.get(IMP_TYPE)));
        result.setCountOfClicks(awaitSuccessfull(futuresByType.get(CLICK_TYPE)));
        return result;
    }
}
