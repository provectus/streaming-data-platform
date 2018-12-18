package com.provectus.fds.compaction.utils;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class S3Utils {
    public static File downloadFile(S3Object object) throws IOException {
        File outputFile = File.createTempFile(object.getBucketName(), object.getKey().replace(Path.SEPARATOR, "_"));
        try (S3ObjectInputStream s3gs = object.getObjectContent();
             InputStream s3is = new GZIPInputStream(s3gs);
             FileOutputStream fos = new FileOutputStream(outputFile)) {
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
        }
        return outputFile;
    }

}
