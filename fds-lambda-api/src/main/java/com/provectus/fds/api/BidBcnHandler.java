package com.provectus.fds.api;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.provectus.fds.models.bcns.Partitioned;
import com.provectus.fds.models.bcns.BidBcn;

import java.io.IOException;
import java.util.Optional;

public class BidBcnHandler extends AbstractBcnHandler {
    @Override
    public Optional<Partitioned> buildBcn(JsonNode parameters, Context context) throws IOException {
        Optional<Partitioned> result =  Optional.empty();
        if (parameters.has("tx_id") && parameters.has("app_uid") && parameters.has("campaign_item_id")) {
            String txid = parameters.get("tx_id").asText();
            String appuid = parameters.get("app_uid").asText();
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
                    new BidBcn(
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
