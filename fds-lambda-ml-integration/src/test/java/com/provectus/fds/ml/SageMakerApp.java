package com.provectus.fds.ml;

import com.amazonaws.services.sagemaker.AmazonSageMaker;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsync;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsyncClient;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsyncClientBuilder;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntime;
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntimeClientBuilder;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest;
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointResult;
import com.provectus.fds.ml.utils.IntegrationModuleHelper;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SageMakerApp {
    public static void main(String[] args) throws IOException {
        AmazonSageMakerRuntime runtime
                = AmazonSageMakerRuntimeClientBuilder.defaultClient();

        String body = "0.2586735022925024,0.7643975138206263,0.7996895421292215,0.0013723629560577523,0.6791536817362743";

        ByteBuffer bodyBuffer = ByteBuffer.wrap(body.getBytes());


        InvokeEndpointRequest request = new InvokeEndpointRequest()
                .withEndpointName("integrationa457ff7da52045189ccEndpoint")
                .withContentType("text/csv")
                .withBody(bodyBuffer);

        InvokeEndpointResult invokeEndpointResult = runtime.invokeEndpoint(request);

        String bodyResponse = new String(invokeEndpointResult.getBody().array());
        System.out.println(bodyResponse);
    }
}
