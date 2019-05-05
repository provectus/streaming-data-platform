package com.provectus.fds.it;

import com.provectus.fds.dynamodb.models.Aggregation;
import com.provectus.fds.it.aws.CloudFormation;
import org.asynchttpclient.Response;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;

public class AwsFdsMLTestIT extends AbstarctFdsTestIt {
    @BeforeClass
    public static void beforeClass() throws Exception {
        String stackName = String.format("%s%s", STACK_NAME_PREFIX, UUID.randomUUID().toString().replace("-", "")).substring(0, 30);
        String bucketName = String.format("fds%s", stackName);

        cloudFormation = new CloudFormation(REGION, stackName
                , new File("fds.yaml"), TEMPLATE_BUCKET
        );
        reportUrl = cloudFormation.getOutput(URL_FOR_REPORTS).getOutputValue();
        apiUrl = cloudFormation.getOutput(URL_FOR_API).getOutputValue();

        String s3EventCommandLine =
                String.format("sam local generate-event s3 put --bucket %s --key parquet/ --region %s | xclip -selection clipboard", bucketName, REGION);
        System.out.println("You can generate an event for this stack with comand:");
        System.out.println(s3EventCommandLine);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        // "note" - is asaushkin's notebook hostname
        // We don't drop the cloudformation stack in this case
        if (!System.getenv().getOrDefault("HOSTNAME", "").equals("note")) {
            if (cloudFormation != null)
                cloudFormation.close();
        }
    }

    @Test
    public void testStackOutputs() {
        assertNotNull(reportUrl);
        assertNotNull(apiUrl);
    }

    @Test
    public void testDynamoTotalReport() throws IOException, ExecutionException, InterruptedException {
        SampleDataResult sampleData = generateSampleData();

        await().atMost(20, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.SECONDS)
                .until(() -> {
                    Aggregation report = getReport(sampleData.getCampaignItemId());
                    return sampleData.getCountOfBids() == report.getBids() &&
                            sampleData.getCountOfImpressions() == report.getImps() &&
                            sampleData.getCountOfClicks() == report.getClicks();
                });
    }


    private Aggregation getReport(long campaignItemId) throws IOException, ExecutionException, InterruptedException {
        Response response = httpClient.prepareGet(reportUrl + "/reports/campaigns/" + campaignItemId)
                .addHeader("User-Agent", USER_AGENT)
                .execute()
                .get();
        return objectMapper.readValue(response.getResponseBodyAsBytes(), Aggregation.class);
    }
}
