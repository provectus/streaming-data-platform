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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.StringJoiner;

@SuppressWarnings("unused")
public class InvokeEndpointLambda implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger logger = LogManager.getLogger(InvokeEndpointLambda.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        try {
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            PredictRequest r = objectMapper.readValue(input.getBody(), PredictRequest.class);

            logger.info("Got a prediction request: {}", r);

            AmazonSageMakerRuntime runtime
                    = AmazonSageMakerRuntimeClientBuilder
                    .standard()
                    .withRegion(System.getenv("REGION"))
                    .build();

            StringJoiner joiner = new StringJoiner(",");
            joiner.add(String.valueOf(r.getCategorizedCampaignItemId()))
                    .add(String.valueOf(r.getCategorizedDomain()))
                    .add(String.valueOf(r.getCategorizedCreativeId()))
                    .add(String.valueOf(r.getCategorizedCreativeCategory()))
                    .add(String.valueOf(r.getWinPrice()));

            logger.info("Invoke the request: {}", joiner);

            ByteBuffer bodyBuffer
                    = ByteBuffer.wrap(joiner.toString().getBytes(Charset.forName("UTF-8")));

            InvokeEndpointRequest request = new InvokeEndpointRequest()
                    .withEndpointName(System.getenv("ENDPOINT"))
                    .withContentType("text/csv")
                    .withBody(bodyBuffer);

            InvokeEndpointResult invokeEndpointResult = runtime.invokeEndpoint(request);

            String bodyResponse = new String(invokeEndpointResult.getBody().array());

            logger.info("Got the prediction answer: {}", bodyResponse);

            APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();
            responseEvent.setBody(bodyResponse);
            responseEvent.setStatusCode(200);

            return responseEvent;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
