package com.provectus.fds.api;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.provectus.fds.models.bcns.Bcn;
import com.provectus.fds.models.bcns.Bid;

import java.io.IOException;
import java.util.Optional;

public class BidBcnHandler extends AbstractBcnHandler {
    @Override
    public Optional<Bcn> buildBcn(JsonNode parameters, Context context) throws IOException {
        Optional<Bcn> result =  Optional.empty();
        if (parameters.has("txid") && parameters.has("appuid") && parameters.has("campaign_item_id")) {
            String txid = parameters.get("txid").asText();
            String appuid = parameters.get("appuid").asText();
            long campaignItemId = parameters.get("campaign_item_id").asLong();
            String domain = "";
            if (parameters.has("domain")) {
                domain = parameters.get("domain").asText();
            }
            String creativeId = "";
            if (parameters.has("creative_id")) {
                creativeId = parameters.get("creative_id").asText();
            }

            String creativeCategory = "";
            if (parameters.has("creative_category")) {
                creativeCategory = parameters.get("creative_category").asText();
            }

            result = Optional.of(
                    new Bid(
                            txid,
                            campaignItemId,
                            domain,
                            creativeId,
                            creativeCategory,
                            appuid
                    )
            );
        }

        return result;
    }
}
