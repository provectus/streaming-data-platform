package com.provectus.fds.compaction.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.provectus.fds.compaction.utils.ParquetTripletUtils.consumeTriplet;
import static org.apache.parquet.column.ParquetProperties.DEFAULT_PAGE_SIZE;

public class ParquetUtils {

    private final Configuration configuration = new Configuration();
    private final CodecFactory.BytesCompressor compressor = new CodecFactory(configuration, DEFAULT_PAGE_SIZE)
            .getCompressor(org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP);

    private final ByteBufferAllocator allocator = new HeapByteBufferAllocator();


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
                 JsonFileReader reader = new JsonFileReader(schema, jsonFile)
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

    public FileMetaData fileMetaData(File file) throws IOException {
        Path parquetPath = new Path("file://" + file.getAbsolutePath());
        return mergedMetadata(Arrays.asList(parquetPath));
    }


    public void mergeFiles(List<Path> inputFiles, Path outputFile) throws IOException {
        this.mergeFiles(ParquetWriter.DEFAULT_BLOCK_SIZE,inputFiles,outputFile);
    }

    public FileMetaData mergeFiles(int maxBlockSize, List<Path> inputFiles, Path outputFile) throws IOException {
        // Merge schema and extraMeta
        FileMetaData mergedMeta = mergedMetadata(inputFiles);

        // Merge data
        this.merge(inputFiles, outputFile, mergedMeta, maxBlockSize);

        return mergedMeta;
    }


    public void merge(List<Path> inputFiles, Path outputFile, FileMetaData fileMetaData, long maxBlockSize) throws IOException {
        MessageType schema = fileMetaData.getSchema();
        ParquetFileWriter writer = new ParquetFileWriter(configuration, schema, outputFile);
        ColumnReadStorePublicImpl columnReadStore = new ColumnReadStorePublicImpl(null, new DummyRecordConverter(schema).getRootConverter(), schema, fileMetaData.getCreatedBy());
        writer.start();

        BlocksCombiner blocksCombiner = new BlocksCombiner(inputFiles, maxBlockSize, configuration);

        List<BlocksCombiner.SmallBlocksUnion> largeBlocks = blocksCombiner.combineLargeBlocks();

        long startMergingFiles = System.currentTimeMillis();
        for (BlocksCombiner.SmallBlocksUnion smallBlocks : largeBlocks) {
            try {
                for (int columnIndex = 0; columnIndex < schema.getColumns().size(); columnIndex++) {
                    ColumnDescriptor path = schema.getColumns().get(columnIndex);

                    ColumnChunkPageWriteStore store = new ColumnChunkPageWriteStore(compressor, schema, allocator);
                    ColumnWriteStoreV1 columnWriteStoreV1 = new ColumnWriteStoreV1(store, ParquetProperties.builder().build());
                    for (BlocksCombiner.SmallBlock smallBlock : smallBlocks.getBlocks()) {
                        try (ParquetFileReader parquetFileReader = smallBlock.getReader()) {
                            Optional<PageReader> columnChunkPageReader = parquetFileReader.readColumnInBlock(smallBlock.getBlockIndex(), path);
                            ColumnWriter columnWriter = columnWriteStoreV1.getColumnWriter(path);
                            if (columnChunkPageReader.isPresent()) {
                                ColumnReader columnReader = columnReadStore.newMemColumnReader(path, columnChunkPageReader.get());
                                for (int i = 0; i < columnReader.getTotalValueCount(); i++) {
                                    consumeTriplet(columnWriter, columnReader);
                                }
                            } else {
                                MessageType inputFileSchema = parquetFileReader.getFileMetaData().getSchema();
                                String[] parentPath = getExisingParentPath(path, inputFileSchema);
                                int def = parquetFileReader.getFileMetaData().getSchema().getMaxDefinitionLevel(parentPath);
                                int rep = parquetFileReader.getFileMetaData().getSchema().getMaxRepetitionLevel(parentPath);
                                for (int i = 0; i < parquetFileReader.getBlockMetaData(smallBlock.getBlockIndex()).getRowCount(); i++) {
                                    columnWriter.writeNull(rep, def);
                                }
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (columnIndex == 0) {
                        writer.startBlock(smallBlocks.getRowCount());
                    }
                    columnWriteStoreV1.flush();
                    columnWriteStoreV1.close();
                    store.flushToFileWriter(path, writer);
                }
                writer.endBlock();
            } catch (Exception e) {

            }
        }
        writer.end(new HashMap<>());
    }

    private String[] getExisingParentPath(ColumnDescriptor path, MessageType inputFileSchema) {
        List<String> parentPath = Arrays.asList(path.getPath());
        while (parentPath.size() > 0 && !inputFileSchema.containsPath(parentPath.toArray(new String[parentPath.size()]))) {
            parentPath = parentPath.subList(0, parentPath.size() - 1);
        }
        return parentPath.toArray(new String[parentPath.size()]);
    }



    private FileMetaData mergedMetadata(List<Path> inputFiles) throws IOException {
        return mergeMetadataFiles(inputFiles, configuration).getFileMetaData();
    }

    public static ParquetMetadata mergeMetadataFiles(List<Path> files, Configuration conf) throws IOException {
        Preconditions.checkArgument(!files.isEmpty(), "Cannot merge an empty list of metadata");

        GlobalMetaData globalMetaData = null;
        List<BlockMetaData> blocks = new ArrayList<BlockMetaData>();

        for (Path p : files) {
            ParquetMetadata pmd = ParquetFileReader.readFooter(conf, p, ParquetMetadataConverter.NO_FILTER);
            FileMetaData fmd = pmd.getFileMetaData();
            globalMetaData = mergeInto(fmd, globalMetaData, true);
            blocks.addAll(pmd.getBlocks());
        }

        // collapse GlobalMetaData into a single FileMetaData, which will throw if they are not compatible
        return new ParquetMetadata(globalMetaData.merge(), blocks);
    }

    private static GlobalMetaData mergeInto(
            FileMetaData toMerge,
            GlobalMetaData mergedMetadata,
            boolean strict) {
        MessageType schema = null;
        Map<String, Set<String>> newKeyValues = new HashMap<String, Set<String>>();
        Set<String> createdBy = new HashSet<String>();
        if (mergedMetadata != null) {
            schema = mergedMetadata.getSchema();
            newKeyValues.putAll(mergedMetadata.getKeyValueMetaData());
            createdBy.addAll(mergedMetadata.getCreatedBy());
        }
        if ((schema == null && toMerge.getSchema() != null)
                || (schema != null && !schema.equals(toMerge.getSchema()))) {
            schema = mergeInto(toMerge.getSchema(), schema, strict);
        }
//        for (Map.Entry<String, String> entry : toMerge.getKeyValueMetaData().entrySet()) {
//            Set<String> values = newKeyValues.get(entry.getKey());
//            if (values == null) {
//                values = new LinkedHashSet<>();
//                newKeyValues.put(entry.getKey(), values);
//            }
//            values.add(entry.getValue());
//        }
        createdBy.add(toMerge.getCreatedBy());
        return new GlobalMetaData(
                schema,
                newKeyValues,
                createdBy);
    }

    private static MessageType mergeInto(MessageType toMerge, MessageType mergedSchema, boolean strict) {
        if (mergedSchema == null) {
            return toMerge;
        }

        return mergedSchema.union(toMerge, strict);
    }

}
