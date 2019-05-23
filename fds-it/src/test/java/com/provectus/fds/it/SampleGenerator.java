package com.provectus.fds.it;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.models.bcns.BidBcn;
import com.provectus.fds.models.bcns.ClickBcn;
import com.provectus.fds.models.bcns.ImpressionBcn;
import com.provectus.fds.models.events.Location;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static com.provectus.fds.it.ItConfig.*;

public class SampleGenerator {
    private int minBids;
    private int maxBids;
    private String apiUrl;

    private AsyncHttpClient httpClient;
    private ObjectMapper objectMapper;

    public SampleGenerator(int minBids, int maxBids, String apiUrl, AsyncHttpClient httpClient, ObjectMapper objectMapper) {
        this.minBids = minBids;
        this.maxBids = maxBids;
        this.apiUrl = apiUrl;

        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    <T> ListenableFuture<Response> sendRequest(String type, T model) throws JsonProcessingException {
        ListenableFuture<Response> future = httpClient.preparePost(String.format("%s/%s", apiUrl, type))
                .addHeader("Content-Type", "application/json")
                .addHeader("User-Agent", USER_AGENT)
                .setBody(objectMapper.writeValueAsBytes(model))
                .execute();
        return future;
    }

    int awaitSuccessfull(List<ListenableFuture<Response>> futures) {
        int n = 0;
        for (ListenableFuture<Response> future : futures) {
            try {
                Response response = future.get();
                //System.out.println(response.toString());
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

        int numberOfBids = random.nextInt(minBids, maxBids);
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

            BidBcn bid = BidBcn.builder()
                    .txId(txid)
                    .campaignItemId(campaignItemId)
                    .domain(domain)
                    .creativeId(creativeId)
                    .creativeCategory(creativeCategory)
                    .appUID(appuid)
                    .build();
            ImpressionBcn impressionBcn = new ImpressionBcn(txid, winPrice);
            ClickBcn clickBcn = new ClickBcn(txid);
            Location location = new Location(appuid, System.currentTimeMillis(), 55.796506, 49.108451);

            futuresByType.computeIfAbsent(BID_TYPE, (k) -> new ArrayList<>())
                    .add(sendRequest(BID_TYPE, bid));

            futuresByType.computeIfAbsent(LOCATION_TYPE, (k) -> new ArrayList<>())
                    .add(sendRequest(LOCATION_TYPE, location));

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

        awaitSuccessfull(futuresByType.get(LOCATION_TYPE));

        return result;
    }
}
