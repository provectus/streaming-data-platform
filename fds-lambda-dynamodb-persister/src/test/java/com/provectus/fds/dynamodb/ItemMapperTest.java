package com.provectus.fds.dynamodb;


import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.models.events.Aggregation;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ItemMapperTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    private List<Aggregation> aggregations = Arrays.asList(
            new Aggregation(1000L, "2008-02-20 10:15:00", 1, 0, 0),
            new Aggregation(1000L, "2008-02-20 10:15:00.000", 1, 0, 0)
    );
    private ItemMapper itemMapper = new ItemMapper();

    @Test
    public void testMapper() throws Exception {

        for (Aggregation aggregation : aggregations) {
            byte[] bytes = mapper.writeValueAsBytes(aggregation);
            Item item = itemMapper.fromByteBuffer(ByteBuffer.wrap(bytes));
            assertEquals(aggregation.getCampaignItemId(), item.getLong("campaign_item_id"));
            //assertEquals(aggregation.getPeriod(), item.getString("period"));
            assertEquals(aggregation.getClicks(), item.getLong("clicks"));
            assertEquals(aggregation.getImps(), item.getLong("imps"));
            assertEquals(aggregation.getBids(), item.getLong("bids"));

        }
    }

    @Test
    public void key() throws JsonProcessingException {
        for (Aggregation aggregation : aggregations) {
            byte[] bytes = mapper.writeValueAsBytes(aggregation);
            PrimaryKey primaryKey = itemMapper.key(ByteBuffer.wrap(bytes));

            assertTrue(primaryKey.hasComponent(ItemMapper.CAMPAIGN_TABLE_HASH_KEY));
            assertTrue(primaryKey.hasComponent(ItemMapper.PERIOD_TABLE_RANGE_KEY));
        }
    }


    @Test
    public void primaryKey() throws JsonProcessingException {
        for (Aggregation aggregation : aggregations) {
            byte[] bytes = mapper.writeValueAsBytes(aggregation);
            Item item = itemMapper.fromByteBuffer(ByteBuffer.wrap(bytes));
            assertEquals(itemMapper.primaryKey(item), itemMapper.key(ByteBuffer.wrap(bytes)));
        }

    }

    @Test
    public void mergeItems() {
    }

    @Test
    public void mergeItem() {
    }

    @Test
    public void mergeValue() {
    }

}