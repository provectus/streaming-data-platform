package com.provectus.fds.compaction.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.parquet.column.ParquetProperties.DEFAULT_PAGE_SIZE;

public class ParquetUtils {

    private final Configuration configuration = new Configuration();
    private final CodecFactory.BytesCompressor compressor = new CodecFactory(configuration, DEFAULT_PAGE_SIZE)
            .getCompressor(org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP);


    public File convert(File tmpDir, File jsonFile, String prefix) throws IOException {

        Optional<Schema> maybeSchema = JsonUtils.buildSchema(jsonFile);

        File tmpParquetFile = new File(tmpDir, UUID.randomUUID().toString());

        if (maybeSchema.isPresent()) {

            Schema schema = maybeSchema.get();
            Path tmpParquetPath = new Path("file://" + tmpParquetFile.getAbsolutePath());
            FileSystem fs = tmpParquetPath.getFileSystem(configuration);
            fs.setWriteChecksum(false);

            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(tmpParquetPath)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build();
                 JsonFileReader reader = new JsonFileReader(schema, jsonFile);
            ) {
                for (GenericData.Record r : reader) {
                    writer.write(r);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        return tmpParquetFile;
    }


    public void mergeFiles(List<Path> inputFiles, Path outputFile) throws IOException {
        this.mergeFiles(ParquetWriter.DEFAULT_BLOCK_SIZE,inputFiles,outputFile);
    }

    public void mergeFiles(int maxBlockSize, List<Path> inputFiles, Path outputFile) throws IOException {
        // Merge schema and extraMeta
        FileMetaData mergedMeta = mergedMetadata(inputFiles);

        // Merge data
        ParquetFileWriter writer = new ParquetFileWriter(configuration, mergedMeta.getSchema(), outputFile, ParquetFileWriter.Mode.CREATE);
        List<InputFile> inputFileList = inputFiles.stream()
                .map(input -> {
                    try {
                        return HadoopInputFile.fromPath(input, configuration);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
        writer.merge(inputFileList, compressor, mergedMeta.getCreatedBy(), maxBlockSize);
    }

    private FileMetaData mergedMetadata(List<Path> inputFiles) throws IOException {
        return ParquetFileWriter.mergeMetadataFiles(inputFiles, configuration).getFileMetaData();
    }
}
