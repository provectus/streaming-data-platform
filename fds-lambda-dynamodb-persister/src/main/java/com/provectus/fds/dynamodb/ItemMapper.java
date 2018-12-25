package com.provectus.fds.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import static java.time.temporal.ChronoField.*;

public class ItemMapper {
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
                .toFormatter(Locale.ENGLISH)
                .withZone(ZoneOffset.UTC);


    public Item fromByteBuffer(ByteBuffer byteBuffer) {
        try {
            JsonNode node = objectMapper.readTree(byteBuffer.array());
            long campaignItemId = node.get("campaign_item_id").asLong();

            ZonedDateTime dateTime = ZonedDateTime.parse(node.get("timestamp").asText(), AWS_DATE_TIME);


            long day = (dateTime.toInstant().toEpochMilli()) / (24 * 60 * 60 * 1000L);


            Item item = new Item()
                    .withPrimaryKey("campaign_item_id", campaignItemId, "day", day)
                    .withString("timestamp", DateTimeFormatter.ISO_DATE_TIME.format(dateTime));

            Iterator<Map.Entry<String, JsonNode>> iterator = node.fields();
            while (iterator.hasNext()) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                if (!entry.getKey().equals("campaign_item_id") &&
                    !entry.getKey().equals("timestamp")) {

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
