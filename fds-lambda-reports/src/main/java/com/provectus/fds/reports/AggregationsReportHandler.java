package com.provectus.fds.reports;

import com.amazonaws.services.lambda.runtime.Context;
import com.provectus.fds.dynamodb.DynamoDAO;
import com.provectus.fds.dynamodb.models.Aggregation;
import com.provectus.fds.dynamodb.repositories.AggregationRepository;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class AggregationsReportHandler extends AbstractReportHandler {
    private final static String DYNAMO_TABLE_ENV = "DYNAMO_TABLE";
    public static final String DYNAMO_TABLE_DEFAULT = "aggregations";
    public static final String CAMPAIGN_ITEM_ID_FIELD = "campaign_item_id";

    private final AggregationRepository repository;

    public AggregationsReportHandler() {
        this.repository = new AggregationRepository(
                this.client
                , System.getenv().getOrDefault(DYNAMO_TABLE_ENV, DYNAMO_TABLE_DEFAULT)
        );
    }

    public Aggregation getTotal(ExecutionValues values, Context context) {
        long campaignItemId = values.orThrow(values.getPathLong(CAMPAIGN_ITEM_ID_FIELD), CAMPAIGN_ITEM_ID_FIELD);
        return this.repository.total(campaignItemId);
    }


    public List<Aggregation> getByPeriod(ExecutionValues values, Context context) {
        long campaignItemId = values.orThrow(values.getPathLong(CAMPAIGN_ITEM_ID_FIELD), CAMPAIGN_ITEM_ID_FIELD);

        ZoneId zoneId = values.getQueryZone("timezone").orElse(ZoneOffset.UTC);
        ChronoUnit chronoUnit = values.getQueryChronoUnit("period").orElse(ChronoUnit.DAYS);
        ZonedDateTime from = values.getQueryZoneDateTime(zoneId, "from").orElse(Instant.ofEpochSecond(0).atZone(zoneId));
        ZonedDateTime to = values.getQueryZoneDateTime(zoneId, "to").orElse(Instant.now().atZone(zoneId));
        boolean desc = values.getQueryBoolean("desc").orElse(true);

        context.getLogger().log("campaignItemId: "+campaignItemId);
        context.getLogger().log("zoneid: "+zoneId.toString());
        context.getLogger().log("chronoUnit: "+chronoUnit.toString());
        context.getLogger().log("from: "+from.toInstant().getEpochSecond());
        context.getLogger().log("to: "+to.toInstant().getEpochSecond());
        context.getLogger().log("desc: "+desc);

        return this.repository.getGrouped(campaignItemId,chronoUnit,zoneId,from, to, desc);
    }



}
