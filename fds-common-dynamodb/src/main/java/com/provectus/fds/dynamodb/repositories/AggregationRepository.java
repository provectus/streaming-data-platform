package com.provectus.fds.dynamodb.repositories;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.provectus.fds.dynamodb.models.Aggregation;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AggregationRepository {
    private DynamoDBMapper dynamoDBMapper;

    public AggregationRepository(AmazonDynamoDB amazonDynamoDB) {
        dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB);
    }

    public List<Aggregation> getAll(long campaignItemId) {
        Map<String, String> expressionAttributesNames = new HashMap<>();
        expressionAttributesNames.put("#campaign_item_id", "campaign_item_id");

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":campaign_item_id", new AttributeValue().withN(Long.toString(campaignItemId)));

        DynamoDBQueryExpression<Aggregation> queryExpression = new DynamoDBQueryExpression<Aggregation>()
                .withKeyConditionExpression("#campaign_item_id = :campaign_item_id")
                .withExpressionAttributeNames(expressionAttributesNames)
                .withExpressionAttributeValues(expressionAttributeValues);

        return dynamoDBMapper.query(Aggregation.class, queryExpression);
    }

    public List<Aggregation> getAllByPeriod(long campaignItemId, ZonedDateTime from, ZonedDateTime to) {
        Map<String, String> expressionAttributesNames = new HashMap<>();
        expressionAttributesNames.put("#campaign_item_id", "campaign_item_id");
        expressionAttributesNames.put("#period", "period");

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":campaign_item_id", new AttributeValue().withN(Long.toString(campaignItemId)));

        expressionAttributeValues.put(":from", new AttributeValue().withN(Long.toString(
                from.toInstant().getEpochSecond()
        )));

        expressionAttributeValues.put(":to", new AttributeValue().withN(Long.toString(
                to.toInstant().getEpochSecond()
        )));


        DynamoDBQueryExpression<Aggregation> queryExpression = new DynamoDBQueryExpression<Aggregation>()
                .withKeyConditionExpression("#campaign_item_id = :campaign_item_id and #period BETWEEN :from AND :to")
                .withExpressionAttributeNames(expressionAttributesNames)
                .withExpressionAttributeValues(expressionAttributeValues);

        return dynamoDBMapper.query(Aggregation.class, queryExpression);
    }

    public Aggregation total(long campaignItemId) {
        return this.getAll(campaignItemId).stream().reduce(new Aggregation(campaignItemId), this::merge);
    }

    public List<Aggregation> getGrouped(long campaignItemId, ChronoUnit chronoUnit, ZoneId timeZone, ZonedDateTime from, ZonedDateTime to, boolean desc) {
        return this.getGrouped(this.getAllByPeriod(campaignItemId,from,to), chronoUnit, timeZone, desc);
    }

    public List<Aggregation> getGrouped(long campaignItemId, ChronoUnit chronoUnit, ZoneId timeZone, boolean desc) {
        return this.getGrouped(this.getAll(campaignItemId), chronoUnit, timeZone, desc);
    }

    public List<Aggregation> getGrouped(List<Aggregation> aggregations, ChronoUnit chronoUnit, ZoneId timeZone, boolean desc) {
        return aggregations.stream().collect(
                Collectors.toMap(
                        i -> i.getDateTime(timeZone, chronoUnit),
                        i -> i,
                        this::merge
                )
        ).entrySet().stream().map( e -> {
            e.getValue().setPeriod(e.getKey().toInstant().getEpochSecond());
            return e.getValue();
        } ).sorted(this.sorter(desc)).collect(Collectors.toList());
    }

    private Comparator<Aggregation> sorter(boolean desc) {
        return (o1, o2) -> {
            long left = o1.getPeriod();
            long right = o2.getPeriod();
            if (desc) {
                left = o2.getPeriod();
                right = o1.getPeriod();
            }
            return Long.compare(left, right);
        };
    }


    private Aggregation merge(Aggregation left, Aggregation right) {
        left.addAggregation(right);
        return left;
    }
}
