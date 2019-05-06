package com.provectus.fds.utils;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.json.JSONObject;
import software.amazon.awssdk.services.kinesisanalyticsv2.KinesisAnalyticsV2Client;
import software.amazon.awssdk.services.kinesisanalyticsv2.model.ApplicationRestoreConfiguration;
import software.amazon.awssdk.services.kinesisanalyticsv2.model.ApplicationRestoreType;
import software.amazon.awssdk.services.kinesisanalyticsv2.model.RunConfiguration;
import software.amazon.awssdk.services.kinesisanalyticsv2.model.StartApplicationRequest;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;

public class ApplicationStartLambda implements RequestHandler<Map<String, Object>, Object> {

    @Override
    public Object handleRequest(Map<String, Object> input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Input: " + input);
        LinkedHashMap properties = (LinkedHashMap) input.get("ResourceProperties");
        String appName = (String) properties.get("ApplicationName");
        String requestType = (String) input.get("RequestType");
        JSONObject responseData = new JSONObject();


        if (requestType.equalsIgnoreCase("Create") || requestType.equalsIgnoreCase("Update")) {
            KinesisAnalyticsV2Client kac = KinesisAnalyticsV2Client.create();
            kac.startApplication(StartApplicationRequest.builder()
                    .applicationName(appName)
                    .runConfiguration(RunConfiguration.builder()
                            .applicationRestoreConfiguration(
                                    ApplicationRestoreConfiguration.builder()
                                            .applicationRestoreType(
                                                    ApplicationRestoreType.RESTORE_FROM_LATEST_SNAPSHOT)
                                            .build())
                            .build())
                    .build());
            logger.log(String.format("The %s was started", appName));
            sendResponse(input, context, "SUCCESS", responseData);
        } else if (requestType.equalsIgnoreCase("DELETE")) {
            logger.log("Resource delete action");
            sendResponse(input, context, "SUCCESS", responseData);
        } else {
            sendResponse(input, context, "SUCCESS", responseData);
        }

        return null;
    }

    private Object sendResponse(
            Map<String, Object> input,
            Context context,
            String responseStatus,
            JSONObject responseData) {

        String responseUrl = (String) input.get("ResponseURL");
        context.getLogger().log("ResponseURL: " + responseUrl);

        URL url;
        try {
            url = new URL(responseUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("PUT");

            JSONObject responseBody = new JSONObject();
            responseBody.put("Status", responseStatus);
            responseBody.put("PhysicalResourceId", context.getLogStreamName());
            responseBody.put("StackId", input.get("StackId"));
            responseBody.put("RequestId", input.get("RequestId"));
            responseBody.put("LogicalResourceId", input.get("LogicalResourceId"));
            responseBody.put("Data", responseData);

            OutputStreamWriter response = new OutputStreamWriter(connection.getOutputStream());
            response.write(responseBody.toString());
            response.close();
            context.getLogger().log("Response Code: " + connection.getResponseCode());

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

}
