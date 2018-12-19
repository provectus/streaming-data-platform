package com.provectus.fds.api;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.provectus.fds.models.bcns.Bcn;
import com.provectus.fds.models.bcns.ClickBcn;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

public class ClickBcnHandler extends AbstractBcnHandler {

    @Override
    public Optional<Bcn> buildBcn(JsonNode parameters, Context context) throws IOException {
        Optional<Bcn> result = Optional.empty();
        if (parameters.has("txid")) {
            context.getLogger().log(String.format("Processing: %s", parameters.toString()));
            String txid = parameters.get("txid").asText();

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
