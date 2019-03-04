package com.provectus.fds.api;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.provectus.fds.models.bcns.Partitioned;
import com.provectus.fds.models.bcns.ClickBcn;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

public class ClickBcnHandler extends AbstractBcnHandler {

    @Override
    public Optional<Partitioned> buildBcn(JsonNode parameters, Context context) throws IOException {
        Optional<Partitioned> result = Optional.empty();
        if (parameters.has("txId")) {
            String txid = parameters.get("txId").asText();

            result = Optional.of(
                    new ClickBcn(
                            txid,
                            Instant.now().toEpochMilli()
                    )
            );
        }
        return result;
    }
}
