package com.provectus.fds.compaction;

import com.provectus.fds.compaction.utils.JsonUtils;
import org.apache.avro.Schema;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSchemaBuilder {
    @Test
    public void test() throws IOException {

        ClassLoader classLoader = getClass().getClassLoader();
        File testBcnFile = new File(classLoader.getResource("testbcn.json").getFile());

        Optional<Schema> schema = JsonUtils.buildSchema(testBcnFile);
        assertTrue(schema.isPresent());

        assertEquals(Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL)
                ,Schema.create(Schema.Type.STRING))
                ),schema.get().getField("type").schema()
        );

        assertEquals(Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL)
                ,Schema.create(Schema.Type.LONG))
                ),schema.get().getField("timestamp").schema()
        );

        assertEquals(Schema.createUnion(Arrays.asList(
                 Schema.create(Schema.Type.NULL)
                ,Schema.create(Schema.Type.LONG))
          ),schema.get().getField("campaign_item_id").schema()
        );

        assertEquals(Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.STRING))
        ),schema.get().getField("domain").schema());

        assertEquals(Schema.createUnion(Arrays.asList(
                Schema.create(Schema.Type.NULL)
                ,Schema.create(Schema.Type.STRING))
                ),schema.get().getField("txid").schema()
        );
    }
}
