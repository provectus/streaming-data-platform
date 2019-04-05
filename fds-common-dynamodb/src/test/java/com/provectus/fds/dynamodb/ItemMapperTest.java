package com.provectus.fds.dynamodb;


import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.models.events.Aggregation;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ItemMapperTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    private List<Aggregation> aggregations = Arrays.asList(
            new Aggregation(1000L, "2008-02-20 10:15:00", 1L, null, 1L),
            new Aggregation(1000L, "2008-02-20 10:15:00.000", 1L, 1L, null)
    );


    private ItemMapper itemMapper = new ItemMapper();

    @Test
    public void testMapper() throws Exception {

        for (Aggregation aggregation : aggregations) {
            Item item = itemMapper.fromByteBuffer(serialize(aggregation));
            assertEquals((long) aggregation.getCampaignItemId(), item.getLong("campaign_item_id"));
            //assertEquals(aggregation.getPeriod(), item.getString("period"));
            assertEquals((long) aggregation.getClicks(), item.getLong("clicks"));
            assertEquals((long) aggregation.getImps(), item.getLong("imps"));
            assertEquals((long) aggregation.getBids(), item.getLong("bids"));

        }
    }

    @Test
    public void key() throws Exception {
        for (Aggregation aggregation : aggregations) {
            byte[] bytes = mapper.writeValueAsBytes(aggregation);
            PrimaryKey primaryKey = itemMapper.key(ByteBuffer.wrap(bytes));

            assertTrue(primaryKey.hasComponent(ItemMapper.CAMPAIGN_TABLE_HASH_KEY));
            assertTrue(primaryKey.hasComponent(ItemMapper.PERIOD_TABLE_RANGE_KEY));
        }
    }


    @Test
    public void primaryKey() throws Exception {
        for (Aggregation aggregation : aggregations) {
            byte[] bytes = mapper.writeValueAsBytes(aggregation);
            Item item = itemMapper.fromByteBuffer(ByteBuffer.wrap(bytes));
            assertEquals(itemMapper.primaryKey(item), itemMapper.key(ByteBuffer.wrap(bytes)));
        }

    }

    private ByteBuffer serialize(Aggregation aggregation) {
        try {
            return ByteBuffer.wrap(mapper.writeValueAsBytes(aggregation));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void mergeItems() {
        Map<PrimaryKey, Item> merged = itemMapper.mergeItems(
                aggregations.stream()
                        .map(this::serialize)
                        .collect(Collectors.toList())
        );
        assertEquals(1, merged.size());
        Item mergedItem = merged.values().iterator().next();
        Aggregation aggregation1 = aggregations.get(0);
        Aggregation aggregation2 = aggregations.get(1);

        assertEquals(aggregation1.getClicks() + aggregation2.getClicks(), mergedItem.getLong("clicks"));
        assertEquals(aggregation1.getBids() + aggregation2.getBids(), mergedItem.getLong("bids"));
        assertEquals(aggregation1.getImps() + aggregation2.getImps(), mergedItem.getLong("imps"));
    }

    @Test
    public void mergeItem() throws Exception {
        Aggregation aggregation1 = aggregations.get(0);
        Aggregation aggregation2 = aggregations.get(1);
        Item aggregation1_item = itemMapper.fromByteBuffer(serialize(aggregation1));
        Item aggregation2_item = itemMapper.fromByteBuffer(serialize(aggregation2));
        Item resultItem = itemMapper.mergeItem(itemMapper.primaryKey(aggregation1_item), aggregation1_item, aggregation2_item);
        assertEquals(itemMapper.primaryKey(aggregation2_item), itemMapper.primaryKey(resultItem));
        assertEquals(aggregation1.getClicks() + aggregation2.getClicks(), resultItem.getLong("clicks"));
        assertEquals(aggregation1.getBids() + aggregation2.getBids(), resultItem.getLong("bids"));
        assertEquals(aggregation1.getImps() + aggregation2.getImps(), resultItem.getLong("imps"));
    }

    @Test
    public void mergeValue() {
    }

}