package com.provectus.fds.ml;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntime;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeClientBuilder;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointResult;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;

public class InvokeEndpointLambda implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            PredictRequest r = objectMapper.readValue(input.getBody(), PredictRequest.class);

            AmazonSageMakerRuntime runtime
                    = AmazonSageMakerRuntimeClientBuilder
                    .standard()
                    .withRegion(System.getenv("REGION"))
                    .build();

            String body =
                    r.getCategorizedCampaignItemId() + "," +
                    r.getCategorizedDomain() + "," +
                    r.getCategorizedCreativeId() + "," +
                    r.getCategorizedCreativeCategory() + "," +
                    r.getWinPrice();

            ByteBuffer bodyBuffer = ByteBuffer.wrap(body.getBytes());

            InvokeEndpointRequest request = new InvokeEndpointRequest()
                    .withEndpointName(System.getenv("ENDPOINT"))
                    .withContentType("text/csv")
                    .withBody(bodyBuffer);

            InvokeEndpointResult invokeEndpointResult = runtime.invokeEndpoint(request);

            String bodyResponse = new String(invokeEndpointResult.getBody().array());
            APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();
            responseEvent.setBody(bodyResponse);
            responseEvent.setStatusCode(200);

            return responseEvent;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
