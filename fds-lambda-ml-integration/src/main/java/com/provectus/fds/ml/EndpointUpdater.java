package com.provectus.fds.ml;

import com.amazonaws.services.sagemaker.AmazonSageMakerAsync;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsyncClient;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsyncClientBuilder;
import com.amazonaws.services.sagemaker.model.*;
import com.provectus.fds.ml.utils.IntegrationModuleHelper;

import java.util.Collections;
import java.util.List;

public class EndpointUpdater {
    private String dataUrl;
    private String sageMakerRole;
    private String endpointName;

    private String regionId;
    private String algorithm;
    private String instanceType;

    private String modelName;
    private String endpointConfigName;

    UpdateEndpointResult updateEndpoint() {
        AmazonSageMakerAsyncClientBuilder sageMakerBuilder
                = AmazonSageMakerAsyncClient.asyncBuilder();
        AmazonSageMakerAsync sage = sageMakerBuilder.build();

        CreateModelRequest modelRequest = createModel(sage);

        CreateEndpointConfigRequest endpointConfigRequest
                = createEndpointConfiguration(sage, modelRequest);

        UpdateEndpointRequest updateEndpointRequest = new UpdateEndpointRequest()
                .withEndpointConfigName(endpointConfigRequest.getEndpointConfigName())
                .withEndpointName(endpointName);

        return sage.updateEndpoint(updateEndpointRequest);
    }

    private CreateEndpointConfigRequest createEndpointConfiguration(AmazonSageMakerAsync sage, CreateModelRequest modelRequest) {
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
        return endpointConfigRequest;
    }

    private CreateModelRequest createModel(AmazonSageMakerAsync sage) {
        ContainerDefinition containerDefinition = new ContainerDefinition()
                .withModelDataUrl(dataUrl)
                .withImage(new SagemakerAlgorithmsRegistry().getFullImageUri(regionId, algorithm));

        CreateModelRequest modelRequest = new CreateModelRequest()
                .withPrimaryContainer(containerDefinition)
                .withExecutionRoleArn(sageMakerRole)
                .withModelName(modelName);

        sage.createModel(modelRequest);
        return modelRequest;
    }

    public static final class EndpointUpdaterBuilder {
        private String dataUrl;
        private String sageMakerRole;
        private String endpointName;

        private String regionId = "us-west-2";
        private String algorithm = "linear-learner";
        private String instanceType = "ml.t2.large";

        private String endpointConfigName = "EndpointConfig";
        private String modelName = "Model";

        private String servicePrefix;
        private IntegrationModuleHelper helper = new IntegrationModuleHelper();

        EndpointUpdaterBuilder() {
        }

        public static EndpointUpdaterBuilder anEndpointUpdater() {
            return new EndpointUpdaterBuilder();
        }

        public EndpointUpdaterBuilder withDataUrl(String dataUrl) {
            this.dataUrl = dataUrl;
            return this;
        }

        public EndpointUpdaterBuilder withRegionId(String regionId) {
            this.regionId = regionId;
            return this;
        }

        public EndpointUpdaterBuilder withAlgorithm(String algorithm) {
            this.algorithm = algorithm;
            return this;
        }

        public EndpointUpdaterBuilder withSageMakerRole(String sageMakerRole) {
            this.sageMakerRole = sageMakerRole;
            return this;
        }

        public EndpointUpdaterBuilder withModelName(String modelName) {
            this.modelName = modelName;
            return this;
        }

        public EndpointUpdaterBuilder withInstanceType(String instanceType) {
            this.instanceType = instanceType;
            return this;
        }

        public EndpointUpdaterBuilder withEndpointConfigName(String endpointConfigName) {
            this.endpointConfigName = endpointConfigName;
            return this;
        }

        public EndpointUpdaterBuilder withEndpointName(String endpointName) {
            this.endpointName = endpointName;
            return this;
        }

        public EndpointUpdaterBuilder withServicePrefix(String servicePrefix) {
            this.servicePrefix = servicePrefix;
            return this;
        }

        static class EndpointUpdaterBuilderException extends RuntimeException {
            EndpointUpdaterBuilderException(String message) {
                super(message);
            }
        }

        public EndpointUpdater build() {

            if (dataUrl == null || sageMakerRole == null || endpointName == null || servicePrefix == null) {
                throw new EndpointUpdaterBuilderException("One of required fields are not initialized properly.\n" +
                        "Consider initialization of the fields: dataUrl, sageMakerRole, endpointName or servicePrefix");
            }

            EndpointUpdater endpointUpdater = new EndpointUpdater();
            endpointUpdater.regionId = this.regionId;
            endpointUpdater.sageMakerRole = this.sageMakerRole;
            endpointUpdater.dataUrl = this.dataUrl;

            endpointUpdater.endpointName = this.endpointName;
            endpointUpdater.algorithm = this.algorithm;
            endpointUpdater.instanceType = this.instanceType;

            endpointUpdater.endpointConfigName = generateName(this.endpointConfigName);
            endpointUpdater.modelName = generateName(this.modelName);

            return endpointUpdater;
        }

        /**
         * Generate name for the AWS resource
         * @return String by the pattern ${ServicePrefix}-resourceName-${RandomSeed}
         */
        String generateName(String resourceName) {

            if (servicePrefix == null) {
                throw new EndpointUpdaterBuilderException("You must setup servicePrefix to non-null value");
            }

            return String.format("%s-%s-%s", servicePrefix,
                    resourceName, helper.getRandomHexString()).substring(0, 63);
        }
    }
}
