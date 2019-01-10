package com.provectus.fds.it;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.junit.Test;

import java.io.*;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

public class AwsFdsTestIT {
    private static String stackName = "fdsit";
    private static String rootUrl;
    private static String REGION = "us-west-2";

    //@BeforeClass
    public static void beforeClass() throws Exception {
        AmazonCloudFormation stackbuilder = AmazonCloudFormationClientBuilder.standard()
                .withRegion(REGION)
                .build();
        CreateStackRequest createRequest = new CreateStackRequest();
        createRequest.setStackName(stackName);
        createRequest.setTemplateBody(convertStreamToString(new FileInputStream(new File("fds.yaml"))));
        List<String> capabilities = Arrays.asList("CAPABILITY_IAM", "CAPABILITY_AUTO_EXPAND");
        createRequest.setCapabilities(capabilities);
        List<Parameter> parameters = Arrays.asList(
                new Parameter()
                        .withParameterKey("ServicePrefix")
                        .withParameterValue(stackName),
                new Parameter()
                        .withParameterKey("AnalyticalDBName")
                        .withParameterValue(stackName),
                new Parameter()
                        .withParameterKey("S3BucketName")
                        .withParameterValue(String.format("fds%s", stackName))
        );
        createRequest.setParameters(parameters);
        stackbuilder.createStack(createRequest);
        DescribeStacksResult describeStacksResult = stackbuilder.describeStacks(
                new DescribeStacksRequest().withStackName(stackName)
        );
        System.out.println(String.format("Stack creation completed, the stack %s completed with %s",
                stackName,
                waitForCompletion(stackbuilder))
        );
    }

    //@AfterClass
    public static void afterClass() throws Exception {
        AmazonCloudFormation stackbuilder = AmazonCloudFormationClientBuilder.standard()
                .withRegion(REGION)
                .build();
        DeleteStackRequest deleteStackRequest = new DeleteStackRequest();
        deleteStackRequest.setStackName(stackName);
        stackbuilder.deleteStack(deleteStackRequest);
        System.out.println(String.format("Stack deletion completed, the stack %s completed with %s",
                stackName,
                waitForCompletion(stackbuilder))
        );
    }

    @Test
    public void testStackOutputs() {
        System.out.println("testStackOutputs started");
        AmazonCloudFormation stackbuilder = AmazonCloudFormationClientBuilder.standard()
                .withRegion(REGION)
                .build();
        DescribeStacksResult describeStacksResult = stackbuilder.describeStacks(
                new DescribeStacksRequest().withStackName(stackName));
        for (Stack s : describeStacksResult.getStacks()) {
            for (Output i : s.getOutputs()) {
                if (i.getOutputKey().equalsIgnoreCase("RootUrlFor")) {
                    rootUrl = i.getOutputValue();
                }
            }
        }
        assert !rootUrl.isEmpty();
    }

    @Test
    public void testRequests() throws IOException {
        System.out.println("testRequests started");
        int length = 10;
        String appuid = "testAppuid";
        String domain = "testDomain";
        String creativeCategory = "testCreativeCategory";
        String creativeId = "testCreativeId";
        int campaignItemId = 1;
        int countOfBids = 0;
        int countOfClicks = 0;
        int countOfImpressions = 0;
        for (int i = 0; i < length; i++) {
            int txid = 100 + i;
            HttpPost bidRequest = new HttpPost(String.format("%s/bid", rootUrl));
            bidRequest.setEntity(new StringEntity(
                    String.format("{\"win_price\": 1," +
                            " \"appuid\": \"%s\"," +
                            " \"campaign_item_id\": %d," +
                            " \"creative_category\": \"%s\"," +
                            " \"creative_id\":\"%s\"," +
                            " \"txid\":\"%s\"," +
                            " \"domain\":\"%s\"," +
                            " \"type\":\"bid\"}", appuid, campaignItemId, creativeCategory, creativeId, txid, domain)));

            HttpPost clickRequest = new HttpPost(String.format("%s/click", rootUrl));
            clickRequest.setEntity(new StringEntity(
                    String.format("{\"txid\":\"%s\", \"type\":\"click\"}", txid)));

            HttpPost impressionRequest = new HttpPost(String.format("%s/impression", rootUrl));
            impressionRequest.setEntity(new StringEntity(
                    String.format("{\"txid\":\"%s\", \"win_price\": 1, \"type\":\"imp\"}", txid)));


            countOfBids += performRequest(bidRequest);
            countOfClicks += performRequest(clickRequest);
            countOfImpressions += performRequest(impressionRequest);
            System.out.println(String.format("%d;%d;%d", countOfBids, countOfClicks, countOfImpressions));

        }
        assert countOfBids == 10;
        assert countOfClicks == 10;
        assert countOfImpressions == 10;
    }

    private int performRequest(HttpPost request) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpResponse response = httpclient.execute(request);
        return response.getStatusLine().getStatusCode() == 200 ? 1 : 0;
    }

    private static String convertStreamToString(InputStream in) throws Exception {

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder stringbuilder = new StringBuilder();
        String line = null;
        while ((line = reader.readLine()) != null) {
            stringbuilder.append(String.format("%s\n", line));
        }
        in.close();
        return stringbuilder.toString();
    }

    private static String waitForCompletion(AmazonCloudFormation stackbuilder) throws Exception {
        DescribeStacksRequest wait = new DescribeStacksRequest();
        wait.setStackName(stackName);
        boolean completed = false;
        String stackStatus = "Unknown";
        String stackReason = "";
        System.out.print("Waiting");
        while (!completed) {
            List<Stack> stacks = stackbuilder.describeStacks(wait).getStacks();
            if (stacks.isEmpty()) {
                completed = true;
                stackStatus = "NO_SUCH_STACK";
                stackReason = "Stack has been deleted";
            } else {
                for (Stack stack : stacks) {
                    StackStatus currentStackStatus = StackStatus.valueOf(stack.getStackStatus());
                    switch (currentStackStatus) {
                        case CREATE_FAILED:
                        case CREATE_COMPLETE:
                        case ROLLBACK_FAILED:
                        case ROLLBACK_COMPLETE:
                        case DELETE_FAILED:
                        case DELETE_COMPLETE:
                            completed = true;
                            stackStatus = stack.getStackStatus();
                            stackReason = stack.getStackStatusReason();
                            break;
                        default:
                            break;
                    }
                }
            }
            System.out.print(".");
            if (!completed) Thread.sleep(10000);
        }
        System.out.println("done");
        return String.format("%s (%s)", stackStatus, stackReason);
    }
}
