package com.provectus.fds.compaction;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Base64;

import static org.junit.Assert.assertEquals;

public class KinesisFireHoseResponseTest {
    @Test
    public void testNewLineAppender() {
        KinesisFirehoseResponse.FirehoseRecord record =
                KinesisFirehoseResponse.FirehoseRecord
                        .appendNewLine("Test", ByteBuffer.wrap("Test".getBytes()));

        String result = new String(Base64.getDecoder().decode(record.getData()));
        assertEquals("Test" + '\n', result);
    }
}
