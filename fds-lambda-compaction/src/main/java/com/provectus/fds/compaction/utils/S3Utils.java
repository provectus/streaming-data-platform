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
    public static File downloadFile(S3Object object, String fileName) throws IOException {
        File outputFile = File.createTempFile(object.getBucketName(), fileName);
        InputStream s3is = null;

        try (S3ObjectInputStream s3gs = object.getObjectContent();
             FileOutputStream fos = new FileOutputStream(outputFile)) {

            InputStream fileStream = s3gs;

            if (fileName.endsWith(".gz")) {
                s3is = new GZIPInputStream(s3gs);
                fileStream = s3is;
            }

            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = fileStream.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
        } finally {
            if (s3is!=null) {
                s3is.close();
            }
        }
        return outputFile;
    }

}
