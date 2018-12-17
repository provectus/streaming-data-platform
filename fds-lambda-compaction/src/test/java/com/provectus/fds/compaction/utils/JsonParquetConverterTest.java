package com.provectus.fds.compaction.utils;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class JsonParquetConverterTest {

    private final File tmpDir = Files.createTempDirectory("s3").toFile();
    private final ParquetUtils parquetUtils = new ParquetUtils();

    public JsonParquetConverterTest() throws IOException {
        tmpDir.deleteOnExit();
    }

    @Test
    public void testJsonFileConvert() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File testBcnFile = new File(classLoader.getResource("testbcn.json").getFile());
        File result = parquetUtils.convert(tmpDir,testBcnFile, "prefix");
        result.deleteOnExit();
        assertTrue(result.exists());
    }

}