package com.provectus.fds.it.aws;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.*;
import com.amazonaws.services.cloudformation.model.Stack;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.*;
import java.util.*;

public class CloudFormation implements AutoCloseable {
    private final String stackName;
    private final File templateFile;
    private final AmazonCloudFormation stackBuilder;
    private final Stack stack;
    private final Map<String,Output> outputs;
    private final String templateBucket;

    private final String region;
    private String s3bucket;


    public CloudFormation(String region, String stackName, File templateFile, String templateBucket) throws IOException, InterruptedException {
        this.region = region;
        this.stackName = stackName;
        this.templateFile = templateFile;
        this.templateBucket = templateBucket;
        this.stackBuilder = AmazonCloudFormationClientBuilder.standard()
                .withRegion(region)
                .build();
        this.stack = createStack();
        this.outputs = this.getOutputsMap();
    }

    public Output getOutput(String key) {
        return this.outputs.get(key.toLowerCase());
    }

    private Stack createStack() throws InterruptedException {

        CreateStackRequest createRequest = new CreateStackRequest();
        createRequest.setStackName(this.stackName);
        createRequest.setTemplateURL(uploadTemplateToS3());
        List<String> capabilities = Arrays.asList(Capability.CAPABILITY_IAM.name(), Capability.CAPABILITY_AUTO_EXPAND.name());
        createRequest.setCapabilities(capabilities);
        s3bucket = String.format("fds%s", stackName);
        List<Parameter> parameters = Arrays.asList(
                new Parameter()
                        .withParameterKey("ServicePrefix")
                        .withParameterValue(stackName),
                new Parameter()
                        .withParameterKey("AnalyticalDBName")
                        .withParameterValue(stackName),
                new Parameter()
                        .withParameterKey("S3BucketName")
                        .withParameterValue(s3bucket),
                new Parameter()
                        .withParameterKey("AggregationPeriod")
                        .withParameterValue("2")
        );
        createRequest.setParameters(parameters);
        CreateStackResult createResult = this.stackBuilder.createStack(createRequest);


        Stack result = waitForStack(createResult.getStackId()).get();

        System.out.println(String.format("Stack creation completed, the stack %s completed with %s",
                stackName,
                result.getStackStatus()
        ));

        return result;
    }

    private Optional<Stack> waitForStack(String stackId) throws InterruptedException {
        Optional<Stack> result = Optional.empty();
        DescribeStacksRequest waitRequest = new DescribeStacksRequest();
        waitRequest.setStackName(stackId);

        boolean completed = false;

        System.out.print("Waiting");
        while (!completed) {

            List<Stack> stacks = this.stackBuilder.describeStacks(waitRequest).getStacks();

            if (stacks.isEmpty()) {
                completed = true;
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
                            result = Optional.of(stack);
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
        return result;
    }

    private Map<String,Output> getOutputsMap() {
        Map<String,Output> outputs = new HashMap<>();
        for (Output output : this.stack.getOutputs()) {
            outputs.put(output.getOutputKey().toLowerCase(), output);
        }
        return outputs;
    }

    private String uploadTemplateToS3() {
        String templateName = String.format("%s.yaml", stackName);

        AmazonS3Client amazonS3 = (AmazonS3Client) AmazonS3ClientBuilder.defaultClient();
        amazonS3.putObject(templateBucket, templateName, templateFile);
        return String.valueOf(amazonS3.getUrl(templateBucket, templateName));
    }

    public void close() throws Exception {

        /**
         * Firstly, we must remove the bucket because the bucket is not
         * empty now and the stack will not be deleted successfully.
         */
        new BucketRemover().removeBucket(region, s3bucket);
        System.out.println(String.format("S3 bucket %s was removed successfully", s3bucket));

        DeleteStackRequest deleteStackRequest = new DeleteStackRequest();
        deleteStackRequest.setStackName(this.stack.getStackId());
        this.stackBuilder.deleteStack(deleteStackRequest);

        System.out.println(String.format("Stack deletion completed, the stack %s completed with %s",
                stackName,
                waitForStack(this.stack.getStackId()).get().getStackStatus())
        );
    }

}
