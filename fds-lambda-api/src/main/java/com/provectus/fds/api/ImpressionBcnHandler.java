package com.provectus.fds.api;


import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.provectus.fds.models.bcns.Bcn;
import com.provectus.fds.models.bcns.ImpressionBcn;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

public class ImpressionBcnHandler extends AbstractBcnHandler {

    @Override
    public Optional<Bcn> buildBcn(JsonNode parameters, Context context) throws IOException {

        Optional<Bcn> result = Optional.empty();

        if (parameters.has("txid") && parameters.has("win_price")) {
            context.getLogger().log(String.format("Processing: %s", parameters.toString()));
            String txid = parameters.get("txid").asText();
            long winPrice = parameters.get("win_price").asLong();

            result = Optional.of(new ImpressionBcn(
                    txid
                    , Instant.now().toEpochMilli()
                    , winPrice
            ));
        }

        return result;
    }

}
