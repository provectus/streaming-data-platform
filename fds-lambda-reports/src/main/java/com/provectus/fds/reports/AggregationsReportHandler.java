package com.provectus.fds.reports;

import com.provectus.fds.dynamodb.DynamoDAO;
import com.provectus.fds.dynamodb.repositories.AggregationRepository;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

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

    public Object getTotal(ExecutionValues values) {
        long campaignItemId = values.orThrow(values.getPathLong(CAMPAIGN_ITEM_ID_FIELD), CAMPAIGN_ITEM_ID_FIELD);
        return this.repository.total(campaignItemId);
    }


    public Object getByPeriod(ExecutionValues values) {
        long campaignItemId = values.orThrow(values.getPathLong(CAMPAIGN_ITEM_ID_FIELD), CAMPAIGN_ITEM_ID_FIELD);

        ZoneId zoneId = values.getQueryZone("timezone").orElse(ZoneOffset.UTC);
        ChronoUnit chronoUnit = values.getQueryChronoUnit("period").orElse(ChronoUnit.DAYS);
        ZonedDateTime from = values.getQueryZoneDateTime(zoneId, "from").orElse(Instant.ofEpochSecond(0).atZone(zoneId));
        ZonedDateTime to = values.getQueryZoneDateTime(zoneId, "to").orElse(Instant.now().atZone(zoneId));
        boolean desc = values.getQueryBoolean("desc").orElse(true);

        return this.repository.getGrouped(campaignItemId,chronoUnit,zoneId,from, to, desc);
    }



}
