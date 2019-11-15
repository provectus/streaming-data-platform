package com.provectus.fds.it;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.*;
import com.amazonaws.services.cloudformation.model.Stack;
import com.amazonaws.services.s3.AmazonS3Client;

import java.io.*;
import java.util.*;

public class CloudFormation implements AutoCloseable {
    private final String stackName;
    private final File templateFile;
    private final AmazonCloudFormation stackBuilder;
    private final Stack stack;
    private final Map<String,Output> outputs;

    private final String region;
    private String s3bucket;
    private final String servicePrefix;

    public CloudFormation(String region, String stackName, File templateFile, String servicePrefix) throws InterruptedException {
        this.region = region;
        this.stackName = stackName;
        this.templateFile = templateFile;
        this.servicePrefix = servicePrefix;
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

        String resourceBucket = System.getProperty("resourceBucket");
        if (resourceBucket == null) {
            throw new RuntimeException(
                    "System property 'resourceBucket' is null. If you run this code from maven\n" +
                            "then don't forget add key -DresourceBucket=<yourTemporaryBucket>\n" +
                            "If you run this code from IDE, then don't forget setup this property\n" +
                            "in Debug/Run configuration (add -DresourceBucket=<yourTemporaryBucket> in the VM options)"
            );
        }

        CreateStackRequest createRequest = new CreateStackRequest();
        createRequest.setStackName(this.stackName);
        createRequest.setTemplateURL(uploadTemplateToS3(resourceBucket));

        List<String> capabilities = Arrays.asList(Capability.CAPABILITY_IAM.name(), Capability.CAPABILITY_AUTO_EXPAND.name());
        createRequest.setCapabilities(capabilities);

        System.out.println("ServicePrefix is: " + servicePrefix);

        List<Parameter> parameters = Arrays.asList(
                new Parameter()
                        .withParameterKey("AggregationPeriod")
                        .withParameterValue("2"),
                new Parameter()
                        .withParameterKey("ServicePrefix")
                        .withParameterValue(servicePrefix)
        );
        createRequest.setParameters(parameters);
        CreateStackResult createResult = this.stackBuilder.createStack(createRequest);


        Stack result = waitForStack(createResult.getStackId()).get();

        System.out.println(String.format("Stack creation completed, the stack %s completed with %s",
                stackName,
                result.getStackStatus()
        ));
        System.out.println("Outputs:");
        Map<String,Output> outputs = new HashMap<>();
        for (Output output : result.getOutputs()) {
            System.out.println(String.format("%s:%s",output.getOutputKey(),output.getOutputValue()));
            outputs.put(output.getOutputKey().toLowerCase(), output);
        }
        this.s3bucket = outputs.get("Bucket".toLowerCase()).getOutputValue();
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


    /**
     * Copy the template to a resource folder because the template has
     * limitation no more than 51200 bytes.
     *
     * @see <a href="https://docs.aws.amazon.com/en_us/AWSCloudFormation/latest/UserGuide/cloudformation-limits.html">
     *     AWS CloudFormation Limits</a>
     */
    private String uploadTemplateToS3(String s3resourceBucket) {
        String templateName = String.format("%s.yaml", stackName);

        AmazonS3Client amazonS3 = (AmazonS3Client) AmazonS3Client.builder()
                .withRegion(region)
                .build();

        amazonS3.putObject(s3resourceBucket, templateName, templateFile);
        return String.valueOf(amazonS3.getUrl(s3resourceBucket, templateName));
    }

    public void close() throws Exception {

        /**
         * Firstly, we must remove the bucket because the bucket is not
         * empty now and the stack will not be deleted successfully.
         */
        new BucketRemover(region).removeBucketWithRetries(s3bucket, 30);

        DeleteStackRequest deleteStackRequest = new DeleteStackRequest();
        deleteStackRequest.setStackName(this.stack.getStackId());
        this.stackBuilder.deleteStack(deleteStackRequest);

        System.out.println(String.format("Stack deletion completed, the stack %s completed with %s",
                stackName,
                waitForStack(this.stack.getStackId()).get().getStackStatus())
        );
    }

}
