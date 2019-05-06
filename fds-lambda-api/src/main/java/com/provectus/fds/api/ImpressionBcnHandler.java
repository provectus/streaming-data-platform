package com.provectus.fds.api;


import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.provectus.fds.models.bcns.Partitioned;
import com.provectus.fds.models.bcns.ImpressionBcn;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

public class ImpressionBcnHandler extends AbstractBcnHandler {

    @Override
    public Optional<Partitioned> buildBcn(JsonNode parameters, Context context) throws IOException {

        Optional<Partitioned> result = Optional.empty();

        if (parameters.has("tx_id") && parameters.has("win_price")) {
            String txid = parameters.get("tx_id").asText();
            long winPrice = parameters.get("win_price").asLong();

            result = Optional.of(new ImpressionBcn(txid, winPrice));
        }

        return result;
    }

}
