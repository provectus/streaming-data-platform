package com.provectus.fds.api;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.provectus.fds.models.bcns.Bcn;
import com.provectus.fds.models.utils.JsonUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractBcnHandler implements RequestStreamHandler {

    public static final String ENV_STREAM_NAME = "STREAM_NAME";
    public static final String STREAM_NAME_DEFUALT_VALUE = "bcns";
    public static final String ENV_DEFAULT_REGION = "AWS_DEFAULT_REGION";

    private final AtomicReference<AmazonKinesis> amazonKinesisReference = new AtomicReference<>();

    private final String streamName;

    public AbstractBcnHandler() {
        this.streamName = System.getenv().getOrDefault(ENV_STREAM_NAME, STREAM_NAME_DEFUALT_VALUE);
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        context.getLogger().log("Handling request");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            JsonNode inputNode = JsonUtils.readTree(reader);
            if (inputNode.has("queryStringParameters")) {
                JsonNode parameters = inputNode.get("queryStringParameters");
                Optional<Bcn> bcn = this.buildBcn(parameters, context);

                if (bcn.isPresent()) {
                    Bcn rawBcn = bcn.get();

                    this.send(
                            rawBcn.getPartitionKey(),
                            rawBcn.getBytes(),
                            context
                    );
                }
            } else {
                context.getLogger().log(String.format("Wrong request: %s", inputNode.toString()));
            }

        } catch (Throwable e) {
            context.getLogger().log("Error on processing bcn: " + e.getMessage());
        }
        // TODO: German, please fix this crap
        byte[] response = "{\"statusCode\": 200}".getBytes();
        outputStream.write(response);
        context.getLogger().log(response);
    }

    private void send(String partitionKey, byte[] data, Context context) {
        AmazonKinesis client = getKinesisOrBuild();

        PutRecordRequest putRecordRequest = new PutRecordRequest().withStreamName(streamName)
                .withData(ByteBuffer.wrap(data)).withPartitionKey(partitionKey);


        PutRecordResult response = client.putRecord(putRecordRequest);
        context.getLogger().log("Record was sent to Kenesis");
    }

    private AmazonKinesis getKinesisOrBuild() {
        AmazonKinesis result = this.amazonKinesisReference.get();
        if (result == null) {
            result = buildKinesis();
            if (!this.amazonKinesisReference.compareAndSet(null, result)) {
                return this.amazonKinesisReference.get();
            }
        }
        return result;
    }

    private AmazonKinesis buildKinesis() {
        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        clientBuilder.setRegion(System.getenv(ENV_DEFAULT_REGION));

        return clientBuilder.build();
    }


    public abstract Optional<Bcn> buildBcn(JsonNode parameters, Context context) throws IOException;

}
