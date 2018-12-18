package com.provectus.fds.compaction.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PathFormatterTest {
    @Test
    public void testCreate() {
        PathFormatter formatter = PathFormatter.fromS3Path("raw/bcns/2018/12/09/11/filenameuuuid.gz");
        assertEquals("parquet/bcns/year=2018/day=17874", formatter.path("parquet"));
        assertEquals("parquet/bcns/year=2018/day=17874/data.parquet", formatter.pathWithFile("parquet", "data.parquet"));
    }
}