package com.provectus.fds.it;

import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntime;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeClientBuilder;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointResult;
import com.provectus.fds.dynamodb.models.Aggregation;
import com.provectus.fds.it.aws.CloudFormation;
import org.asynchttpclient.Response;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertNotNull;

import static com.provectus.fds.it.ItConfig.*;
import static org.junit.Assert.assertTrue;

public class AwsFdsMLTestIT extends AbstarctFdsTestIt {
    final static String stackName = String.format("%s%s", STACK_NAME_PREFIX, UUID.randomUUID().toString().replace("-", "")).substring(0, 30);

    @BeforeClass
    public static void beforeClass() throws Exception {
        String bucketName = String.format("fds%s", stackName);

        cloudFormation = new CloudFormation(REGION, stackName
                , new File("../fds.yaml"), TEMPLATE_BUCKET
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
        String leaveStack = System.getProperty("leaveStack");

        if (leaveStack == null && cloudFormation != null)
            cloudFormation.close();
    }

    @Test
    public void testStackOutputs() {
        assertNotNull(reportUrl);
        assertNotNull(apiUrl);
    }

    @Test
    public void testDynamoTotalReport() throws IOException, ExecutionException, InterruptedException {
        SampleDataResult sampleData = generateSampleData(300, 400);

        await().atMost(20, TimeUnit.MINUTES)
                .pollInterval(10, TimeUnit.SECONDS)
                .until(() -> {
                    Aggregation report = getReport(sampleData.getCampaignItemId());
                    return sampleData.getCountOfBids() == report.getBids() &&
                            sampleData.getCountOfImpressions() == report.getImps() &&
                            sampleData.getCountOfClicks() == report.getClicks();
                });
    }

    @Test
    public void testEndpoint() {
        AmazonSageMakerRuntime runtime
                = AmazonSageMakerRuntimeClientBuilder.defaultClient();

        String body = "0.2586735022925024,0.7643975138206263,0.7996895421292215,0.0013723629560577523,900";

        ByteBuffer bodyBuffer = ByteBuffer.wrap(body.getBytes());

        InvokeEndpointRequest request = new InvokeEndpointRequest()
                .withEndpointName(String.format("%sEndpoint", stackName))
                .withContentType("text/csv")
                .withBody(bodyBuffer);

        InvokeEndpointResult invokeEndpointResult = runtime.invokeEndpoint(request);

        String bodyResponse = new String(invokeEndpointResult.getBody().array());

        // TODO: Make it more sophisticated
        assertTrue(bodyResponse.contains("predictions"));
    }

    private Aggregation getReport(long campaignItemId) throws IOException, ExecutionException, InterruptedException {
        Response response = httpClient.prepareGet(reportUrl + "/reports/campaigns/" + campaignItemId)
                .addHeader("User-Agent", USER_AGENT)
                .execute()
                .get();
        return objectMapper.readValue(response.getResponseBodyAsBytes(), Aggregation.class);
    }
}
