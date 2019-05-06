package com.provectus.fds.ml;

import com.amazonaws.services.sagemaker.AmazonSageMakerAsync;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsyncClient;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsyncClientBuilder;
import com.amazonaws.services.sagemaker.model.*;

import java.util.Collections;
import java.util.List;

public class EndpointUpdater {
    private String dataUrl = "s3://newfdsb/ml/model/linear-learner-2019-04-17-12-30-01-280/output/model.tar.gz";
    private String regionId = "us-west-2";
    private String algorithm = "linear-learner";
    private String sageMakerRole = "role...";
    private String modelName = "JavaAPI-model-name";
    private String instanceType = "ml.t2.large";
    private String endpointConfigName = "newEndpointConfig";
    private String endpointName = "test-endpoint";

    public UpdateEndpointResult updateEndpoint() {
        AmazonSageMakerAsyncClientBuilder sageMakerBuilder
                = AmazonSageMakerAsyncClient.asyncBuilder();
        AmazonSageMakerAsync sage = sageMakerBuilder.build();

        // 1. Create model
        ContainerDefinition containerDefinition = new ContainerDefinition()
                .withModelDataUrl(dataUrl)
                .withImage(new SagemakerAlgorithmsRegistry()
                        .getImageUri(regionId, algorithm) + "/" + algorithm + ":latest");

        CreateModelRequest modelRequest = new CreateModelRequest()
                .withPrimaryContainer(containerDefinition)
                .withExecutionRoleArn(sageMakerRole)
                .withModelName(modelName);

        sage.createModel(modelRequest);

        // 2. Create endpoint configuration
        ProductionVariant variant = new ProductionVariant().withInitialInstanceCount(1)
                .withInitialVariantWeight(1F)
                .withInstanceType(instanceType)
                .withModelName(modelRequest.getModelName())
                .withVariantName(modelRequest.getModelName());

        List<ProductionVariant> productionVariants = Collections.singletonList(variant);
        CreateEndpointConfigRequest endpointConfigRequest = new CreateEndpointConfigRequest()
                .withProductionVariants(productionVariants)
                .withEndpointConfigName(endpointConfigName);
        sage.createEndpointConfig(endpointConfigRequest);

        // 3. Update endpoint with new configuration
        UpdateEndpointRequest updateEndpointRequest = new UpdateEndpointRequest()
                .withEndpointConfigName(endpointConfigRequest.getEndpointConfigName())
                .withEndpointName(endpointName);

        return sage.updateEndpoint(updateEndpointRequest);
    }
}
