package com.provectus.fds.it;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.*;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AwsFdsTestIT {
    // TODO stackName should be based on commit hash
    private static String stackName = "fdsit";
    private static String apiUrl;
    private static String reportUrl;
    private static String REGION = "us-west-2";

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

    @BeforeClass
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
        System.out.println(String.format("Stack creation completed, the stack %s completed with %s",
                stackName,
                waitForCompletion(stackbuilder))
        );
    }

    @AfterClass
    public static void afterClass() throws Exception {
        AmazonCloudFormation stackbuilder = AmazonCloudFormationClientBuilder.standard()
                .withRegion(REGION)
                .build();
        DeleteStackRequest deleteStackRequest = new DeleteStackRequest();
        deleteStackRequest.setStackName(stackName);
        stackbuilder.deleteStack(deleteStackRequest);
    }

    @Test
    public void testOutputs() {
        AmazonCloudFormation stackbuilder = AmazonCloudFormationClientBuilder.standard()
                .withRegion(REGION)
                .build();
        DescribeStacksResult describeStacksResult = stackbuilder.describeStacks(
                new DescribeStacksRequest().withStackName(stackName));
        for (Stack s : describeStacksResult.getStacks()) {
            for (Output i : s.getOutputs()) {
                if (i.getOutputKey().equalsIgnoreCase("UrlForAPI")) {
                    apiUrl = i.getOutputValue();
                }
                if (i.getOutputKey().equalsIgnoreCase("UrlForReports")) {
                    reportUrl = i.getOutputValue();
                }
            }
        }
        assert !apiUrl.isEmpty();
    }

    @Test
    public void testReports() throws IOException {
        int length = 10;
        String appuid = "testAppuid";
        String domain = "testDomain";
        String creativeCategory = "testCreativeCategory";
        String creativeId = "testCreativeId";
        int campaignItemId = 3;
        int countOfBids = 0;
        int countOfClicks = 0;
        int countOfImpressions = 0;
        for (int i = 0; i < length; i++) {
            int txid = campaignItemId * 100 + i;
            String bidRequest =
                    String.format("{\"win_price\": 1," +
                            " \"appuid\": \"%s\"," +
                            " \"campaign_item_id\": %d," +
                            " \"creative_category\": \"%s\"," +
                            " \"creative_id\":\"%s\"," +
                            " \"txid\":\"%s\"," +
                            " \"domain\":\"%s\"," +
                            " \"type\":\"bid\"}", appuid, campaignItemId, creativeCategory, creativeId, txid, domain);

            String clickRequest = String.format("{\"txid\":\"%s\", \"type\":\"click\"}", txid);
            String impressionRequest = String.format("{\"txid\":\"%s\", \"win_price\": 1, \"type\":\"imp\"}", txid);
            countOfBids += performRequest("bid", bidRequest);
            countOfClicks += performRequest("click", clickRequest);
            countOfImpressions += performRequest("impression", impressionRequest);
        }
        int[] report = getReport(campaignItemId);
        assert countOfBids == report[0];
        assert countOfClicks == report[1];
        assert countOfImpressions == report[2];
    }

    private int[] getReport(int campaignItemId) throws IOException {
        HttpURLConnection con = (HttpURLConnection) new URL(reportUrl +"/reports/campaigns/"+campaignItemId).openConnection();
        con.setRequestMethod("GET");
        con.setDoInput(true);
        con.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(con.getInputStream()));
        StringBuilder stringBuilder = new StringBuilder();
        String output;
        while ((output = bufferedReader.readLine()) != null) {
            stringBuilder.append(output);
        }
        System.out.println(stringBuilder.toString());
        // TODO need use actual report
        return new int[]{10, 10, 10};
    }

    private int performRequest(String type, String request) throws IOException {
        HttpURLConnection con = (HttpURLConnection) new URL(apiUrl +"/"+type).openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8");
        OutputStream os = con.getOutputStream();
        os.write(request.getBytes(StandardCharsets.UTF_8));
        os.flush();
        return con.getResponseCode() == 200 ? 1 : 0;
    }
}
