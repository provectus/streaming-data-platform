package com.provectus.fds.it.aws;

import com.amazonaws.services.cloudformation.AmazonCloudFormation;
import com.amazonaws.services.cloudformation.AmazonCloudFormationClientBuilder;
import com.amazonaws.services.cloudformation.model.*;
import com.amazonaws.services.cloudformation.model.Stack;

import java.io.*;
import java.util.*;

public class CloudFormation implements AutoCloseable {
    private final String stackName;
    private final File templateFile;
    private final AmazonCloudFormation stackBuilder;
    private final Stack stack;
    private final Map<String,Output> outputs;


    public CloudFormation(String region, String stackName, File templateFile) throws IOException, InterruptedException {
        this.stackName = stackName;
        this.templateFile = templateFile;
        this.stackBuilder = AmazonCloudFormationClientBuilder.standard()
                .withRegion(region)
                .build();
        this.stack = createStack();
        this.outputs = this.getOutputsMap();
    }

    public Output getOutput(String key) {
        return this.outputs.get(key.toLowerCase());
    }

    private Stack createStack() throws IOException, InterruptedException {

        CreateStackRequest createRequest = new CreateStackRequest();
        createRequest.setStackName(this.stackName);
        createRequest.setTemplateBody(readTemplate());
        List<String> capabilities = Arrays.asList(Capability.CAPABILITY_IAM.name(), Capability.CAPABILITY_AUTO_EXPAND.name());
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
                        .withParameterValue(String.format("fds%s", stackName)),
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


    private String readTemplate() throws IOException {

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(this.templateFile)
                )
        )) {

            StringBuilder sb = new StringBuilder();

            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(String.format("%s\n", line));
            }

            return sb.toString();
        }
    }

    public void close() throws Exception {
        DeleteStackRequest deleteStackRequest = new DeleteStackRequest();
        deleteStackRequest.setStackName(this.stack.getStackId());
        this.stackBuilder.deleteStack(deleteStackRequest);

        System.out.println(String.format("Stack deletion completed, the stack %s completed with %s",
                stackName,
                waitForStack(this.stack.getStackId()).get().getStackStatus())
        );

    }

}
