package com.provectus.fds.dynamodb;


import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.provectus.fds.models.events.Aggregation;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class ItemMapperTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testMapper() throws Exception {
        Aggregation aggregation = new Aggregation();
        aggregation.setCampaignItemId(1000L);
        aggregation.setTimestamp("2008-02-20 10:15:00");
        aggregation.setClicks(1);
        byte[] bytes = mapper.writeValueAsBytes(aggregation);
        ItemMapper mapper = new ItemMapper();
        Item item = mapper.fromByteBuffer(ByteBuffer.wrap(bytes));
        assertEquals(1000L, item.getLong("campaign_item_id"));
        assertEquals(13929, item.getLong("day"));
        assertEquals("2008-02-20T10:15:00Z", item.getString("timestamp"));
        assertEquals(1, item.getLong("clicks"));
    }

}