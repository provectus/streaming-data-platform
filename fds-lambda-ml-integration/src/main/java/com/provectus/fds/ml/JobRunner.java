package com.provectus.fds.ml;

import com.amazonaws.services.sagemaker.AmazonSageMakerAsync;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsyncClient;
import com.amazonaws.services.sagemaker.AmazonSageMakerAsyncClientBuilder;
import com.amazonaws.services.sagemaker.model.*;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class JobRunner {

    private static final String JOB_PREFIX = "fds-training-job-";

    private static final String HYPER_PARAMETER_FEATURE_DIM = "5";
    private static final String HYPER_PARAMETER_PREDICTOR_TYPE = "binary_classifier";
    private static final String HYPER_PARAMETER_MINI_BATCH_SIZE = "128";
    private static final String HYPER_PARAMETER_EPOCHS = "1";

    private static final String TRAINING_ALGORITHM = "linear-learner";
    private static final String TRAINING_INPUT_MODE = "File";

    private static final int RESOURCE_INSTANCE_COUNT = 1;
    private static final String RESOURCE_INSTANCE_TYPE = "ml.m5.large";
    private static final int RESOURCE_VOLUME_SIZE_IN_GB = 30;

    public static final String TRAIN = "train";
    public static final String VALIDATION = "validation";


    CreateTrainingJobResult createJob(PrepareDataForTrainingJobLambda.LambdaConfiguration config,
                                      String trainSource, String validationSource) throws IOException {
        AmazonSageMakerAsyncClientBuilder sageMakerBuilder
                = AmazonSageMakerAsyncClient.asyncBuilder();

        AmazonSageMakerAsync sage = sageMakerBuilder.build();
        CreateTrainingJobRequest req = new CreateTrainingJobRequest();
        req.setTrainingJobName(JOB_PREFIX + Instant.now().getEpochSecond());
        req.setRoleArn(config.getSagemakerRole());

        setHyperParameters(req);
        setAlgorithm(config, req);
        setDataConfig(config, trainSource, validationSource, req);
        setStoppingConditions(req);
        setResources(req);

        return sage.createTrainingJob(req);
    }

    private void setResources(CreateTrainingJobRequest jobRequest) {
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.setInstanceCount(RESOURCE_INSTANCE_COUNT);
        resourceConfig.setInstanceType(RESOURCE_INSTANCE_TYPE);
        resourceConfig.setVolumeSizeInGB(RESOURCE_VOLUME_SIZE_IN_GB);

        jobRequest.setResourceConfig(resourceConfig);
    }

    private void setStoppingConditions(CreateTrainingJobRequest jobRequest) {
        StoppingCondition stoppingCondition = new StoppingCondition();
        stoppingCondition.setMaxRuntimeInSeconds(86400);

        jobRequest.setStoppingCondition(stoppingCondition);
    }

    private void setDataConfig(PrepareDataForTrainingJobLambda.LambdaConfiguration config, String trainSource, String validationSource, CreateTrainingJobRequest jobRequest) {
        Channel trainChannel = createChannel(TRAIN, trainSource);
        Channel validationChannel = createChannel(VALIDATION, validationSource);

        List<Channel> channels = Arrays.asList(trainChannel, validationChannel);
        jobRequest.setInputDataConfig(channels);

        OutputDataConfig outputDataConfig = new OutputDataConfig();
        outputDataConfig.setS3OutputPath(config.getModelOutputPath());
        jobRequest.setOutputDataConfig(outputDataConfig);
    }

    private void setAlgorithm(PrepareDataForTrainingJobLambda.LambdaConfiguration config, CreateTrainingJobRequest jobRequest) {
        AlgorithmSpecification specification = new AlgorithmSpecification();
        SagemakerAlgorithmsRegistry registry = new SagemakerAlgorithmsRegistry();

        specification.setTrainingImage(registry.getFullImageUri(config.getRegion(), TRAINING_ALGORITHM));
        specification.setTrainingInputMode(TRAINING_INPUT_MODE);

        jobRequest.setAlgorithmSpecification(specification);
    }

    private void setHyperParameters(CreateTrainingJobRequest jobRequest) {
        jobRequest.addHyperParametersEntry("feature_dim", HYPER_PARAMETER_FEATURE_DIM);
        jobRequest.addHyperParametersEntry("predictor_type", HYPER_PARAMETER_PREDICTOR_TYPE);
        jobRequest.addHyperParametersEntry("mini_batch_size", HYPER_PARAMETER_MINI_BATCH_SIZE);
        jobRequest.addHyperParametersEntry("epochs", HYPER_PARAMETER_EPOCHS);
    }

    private Channel createChannel(String name, String uri) {
        Channel channel = new Channel();
        channel.setChannelName(name);
        channel.setContentType("text/csv");

        S3DataSource s3ds = new S3DataSource();
        s3ds.setS3Uri(uri);
        s3ds.setS3DataType("S3Prefix");

        DataSource ds = new DataSource();
        ds.setS3DataSource(s3ds);

        channel.setDataSource(ds);
        return channel;
    }
}
