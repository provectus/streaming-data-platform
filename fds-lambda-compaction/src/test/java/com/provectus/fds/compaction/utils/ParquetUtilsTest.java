package com.provectus.fds.compaction.utils;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class ParquetUtilsTest {

    private final File tmpDir = Files.createTempDirectory("s3").toFile();

    public ParquetUtilsTest() throws IOException {
        tmpDir.deleteOnExit();
    }

    @Test
    public void testJsonFileConvert() throws IOException {
        File result1 = null;
        File result2 = null;
        File targetFile = null;

        try {
            ClassLoader classLoader = getClass().getClassLoader();
            File testBcnFile = new File(classLoader.getResource("testbcn.json").getFile());
            File testBcn1File = new File(classLoader.getResource("testbcn1.json").getFile());

            ParquetUtils parquetUtils = new ParquetUtils();

            result1 = parquetUtils.convert(tmpDir,testBcn1File, "prefix");
            result2 = parquetUtils.convert(tmpDir,testBcnFile, "prefix2");


            targetFile = File.createTempFile("prefix", "targetparquetfile");
            targetFile.delete();

            parquetUtils.mergeFiles(
                    Arrays.asList(
                            new Path("file://" + result1.getAbsolutePath()),
                            new Path("file://" + result2.getAbsolutePath())
                    ),
                    new Path("file://"+targetFile.getAbsolutePath())
            );

            assertTrue(targetFile.exists());



        } finally {
            if (result1!=null) result1.delete();
            if (result2!=null) result2.delete();
            if (targetFile!=null) targetFile.delete();

        }
    }
}