package com.provectus.fds.dynamodb.repositories;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AggregationRepository {
    private final DynamoDBMapper dynamoDBMapper;
    private final String tableName;
    private final DynamoDBMapperConfig config;

    public AggregationRepository(AmazonDynamoDB amazonDynamoDB, String tableName) {
        this.dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB);
        this.tableName = tableName;
        this.config = new DynamoDBMapperConfig.Builder().withTableNameOverride(
                DynamoDBMapperConfig.TableNameOverride.withTableNameReplacement(this.tableName)
        ).build();
    }

    public AggregationRepository() {
        this.dynamoDBMapper = null;
        this.tableName =null;
        this.config = null;
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

        return dynamoDBMapper.query(Aggregation.class, queryExpression,config);
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

        return dynamoDBMapper.query(Aggregation.class, queryExpression,config);
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
        ).entrySet().stream()
                .map( e -> e.getValue().withPeriod(e.getKey().toInstant().getEpochSecond()))
                .sorted(this.periodSorter(desc)).collect(Collectors.toList());
    }

    private Comparator<Aggregation> periodSorter(boolean desc) {
        return this.sorter(desc, Aggregation::getPeriod, Long::compare);
    }

    private <T> Comparator<Aggregation> sorter(boolean desc, Function<Aggregation, T> getter, Comparator<T> comparator) {
        return (o1, o2) -> {
            T left;
            T right;

            if (desc) {
                left = getter.apply(o2);
                right = getter.apply(o1);
            } else {
                left = getter.apply(o1);
                right = getter.apply(o2);
            }

            return comparator.compare(left, right);
        };
    }


    private Aggregation merge(Aggregation left, Aggregation right) {
        return left.clone().addAggregation(right);
    }
}
