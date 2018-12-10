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
import java.util.logging.Logger;

public abstract class AbstractBcnHandler implements RequestStreamHandler {

    private static final Logger LOGGER = Logger.getLogger(AbstractBcnHandler.class.getName());
    private final AtomicReference<AmazonKinesis> amazonKinesisReference = new AtomicReference<>();


    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) {
        LOGGER.fine("Handling request");

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            JsonNode inputNode = JsonUtils.readTree(reader);
            if (inputNode.has("queryStringParameters")) {
                JsonNode parameters = inputNode.get("queryStringParameters");
                Optional<Bcn> bcn = this.buildBcn(parameters, context);

                if (bcn.isPresent()) {
                    Bcn rawBcn = bcn.get();

                    this.send(
                            rawBcn.getStreamName(),
                            rawBcn.getPartitionKey(),
                            rawBcn.getBytes(),
                            context
                    );
                }
            }

        } catch (Throwable e) {
            LOGGER.severe("Error on processing impression: " + e.getMessage());
        }


    }

    private void send(String streamName, String partitionKey, byte[] data, Context context) {
        AmazonKinesis client = getKinesisOrBuild();

        PutRecordRequest putRecordRequest = new PutRecordRequest().withStreamName(streamName)
                .withData(ByteBuffer.wrap(data)).withPartitionKey(partitionKey);


        PutRecordResult response = client.putRecord(putRecordRequest);
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
        clientBuilder.setRegion(System.getenv("AWS_DEFAULT_REGION"));

        return clientBuilder.build();
    }


    public abstract Optional<Bcn> buildBcn(JsonNode parameters, Context context) throws IOException;

}
