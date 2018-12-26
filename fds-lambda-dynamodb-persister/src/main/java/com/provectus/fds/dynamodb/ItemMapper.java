package com.provectus.fds.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.*;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoField.*;

public class ItemMapper {
    public static final String CAMPAIGN_TABLE_HASH_KEY = "campaign_item_id";
    public static final String PERIOD_TABLE_RANGE_KEY = "period";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final DateTimeFormatter AWS_DATE =
            new DateTimeFormatterBuilder().appendValue(YEAR, 4, 10,SignStyle.EXCEEDS_PAD)
                    .appendLiteral('-')
                    .appendValue(MONTH_OF_YEAR, 2)
                    .appendLiteral('-')
                    .appendValue(DAY_OF_MONTH, 2)
                    .toFormatter(Locale.ENGLISH)
                    .withZone(ZoneOffset.UTC);


    private static final DateTimeFormatter AWS_DATE_TIME =
            new DateTimeFormatterBuilder().appendValue(YEAR, 4, 10,SignStyle.EXCEEDS_PAD)
                .appendLiteral('-')
                .appendValue(MONTH_OF_YEAR, 2)
                .appendLiteral('-')
                .appendValue(DAY_OF_MONTH, 2)
                .appendLiteral(' ')
                .appendValue(HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 2)
                .optionalStart()
                .appendLiteral('.')
                .appendValue(MILLI_OF_SECOND, 3)
                .optionalEnd()
                .toFormatter(Locale.ENGLISH)
                .withZone(ZoneOffset.UTC);



    public PrimaryKey key(ByteBuffer byteBuffer) {
        try {
            return key(objectMapper.readTree(byteBuffer.array()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public PrimaryKey key(JsonNode node) throws IOException {
        ZonedDateTime dateTime = ZonedDateTime.parse(node.get(PERIOD_TABLE_RANGE_KEY).asText(), AWS_DATE_TIME);
        long campaignItemId = node.get(CAMPAIGN_TABLE_HASH_KEY).asLong();

        return new PrimaryKey(CAMPAIGN_TABLE_HASH_KEY, campaignItemId
                , PERIOD_TABLE_RANGE_KEY, dateTime.toInstant().getEpochSecond());

    }

    public PrimaryKey primaryKey(Item item) {
        long campaignItemId = item.getLong(CAMPAIGN_TABLE_HASH_KEY);
        long period = item.getLong(PERIOD_TABLE_RANGE_KEY);
        return new PrimaryKey(CAMPAIGN_TABLE_HASH_KEY, campaignItemId, PERIOD_TABLE_RANGE_KEY, period);
    }


    public List<Item> mergeItems(List<Item> created, List<Item> read) {
        List<Item> result = new ArrayList<>();

        Map<PrimaryKey, Item> readMap =  read.stream().collect(
                Collectors.toMap(
                        this::primaryKey,
                        i -> i
                )
        );

        for (Item item : created) {
            Item resultItem = item;
            PrimaryKey key = primaryKey(item);

            if (readMap.containsKey(key)) {
                resultItem = mergeItem(key, item, readMap.get(key));
            }
            result.add(resultItem);
        }
        return result;
    }

    public Item mergeItem(PrimaryKey primaryKey, Item newItem, Item oldItem) {
        Item resultItem = new Item().withPrimaryKey(primaryKey);
        for (Map.Entry<String,Object> entry : newItem.attributes()) {
            if (!entry.getKey().equals(CAMPAIGN_TABLE_HASH_KEY) &&
                    !entry.getKey().equals(PERIOD_TABLE_RANGE_KEY)
            ) {
                Object value = entry.getValue();
                if (oldItem.hasAttribute(entry.getKey())) {
                    value = mergeValue(entry.getValue(), oldItem.get(entry.getKey()));
                }
                resultItem.with(entry.getKey(), value);
            }
        }
        return resultItem;
    }

    public Object mergeValue(Object newObject, Object oldValue) {
        Object result = newObject;
        if (newObject instanceof Long) {
            Long oldLong = (Long)oldValue;
            Long newLong = (Long)newObject;
            result = (oldLong+newLong);
        }
        return result;
    }


    public Item fromByteBuffer(ByteBuffer byteBuffer) {
        try {
            JsonNode node = objectMapper.readTree(byteBuffer.array());
            ZonedDateTime dateTime = ZonedDateTime.parse(node.get(PERIOD_TABLE_RANGE_KEY).asText(), AWS_DATE_TIME);

            PrimaryKey key = key(node);

            Item item = new Item().withPrimaryKey(key);


            Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                if (!entry.getKey().equals(CAMPAIGN_TABLE_HASH_KEY) &&
                    !entry.getKey().equals(PERIOD_TABLE_RANGE_KEY)) {

                    if (entry.getValue().isBoolean()) {
                        item = item.withBoolean(entry.getKey(), entry.getValue().asBoolean());
                    } else if (entry.getValue().isInt()) {
                        item = item.withInt(entry.getKey(), entry.getValue().asInt());
                    } else if (entry.getValue().isLong()) {
                        item = item.withLong(entry.getKey(), entry.getValue().asLong());
                    } else if (entry.getValue().isDouble()) {
                        item = item.withDouble(entry.getKey(), entry.getValue().asDouble());
                    } else if (entry.getValue().isFloat()) {
                        item = item.withFloat(entry.getKey(), entry.getValue().floatValue());
                    } else if (entry.getValue().isTextual()) {
                        item = item.withString(entry.getKey(), entry.getValue().asText());
                    }

                }
            }
            return item;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
