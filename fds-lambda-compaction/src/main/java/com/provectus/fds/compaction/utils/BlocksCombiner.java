package com.provectus.fds.compaction.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;

public class BlocksCombiner {

  private final Map<Path, ParquetMetadata> footers = new HashMap<>();
  private final List<Path> inputFiles;
  private final long maxBlockSize;
  private final Configuration configuration;

  public BlocksCombiner(List<Path> inputFiles, long maxBlockSize, Configuration configuration) {
    this.inputFiles = inputFiles;
    this.maxBlockSize = maxBlockSize;
    this.configuration = configuration;
  }

  public List<SmallBlocksUnion> combineLargeBlocks()  {
    List<SmallBlocksUnion> blocks = new ArrayList<>();
    long largeBlockSize = 0;
    long largeBlockRecords = 0;
    List<SmallBlock> smallBlocks = new ArrayList<>();
    for (Path inputFile : inputFiles) {
      try (ParquetFileReader reader = getReader(inputFile, configuration, footers)) {
        for (int blockIndex = 0; blockIndex < reader.blocksCount(); blockIndex++) {
          BlockMetaData block = reader.getBlockMetaData(blockIndex);
          if (!smallBlocks.isEmpty() && largeBlockSize + block.getTotalByteSize() > maxBlockSize) {
            blocks.add(new SmallBlocksUnion(smallBlocks, largeBlockRecords));
            smallBlocks = new ArrayList<>();
            largeBlockSize = 0;
            largeBlockRecords = 0;
          }
          largeBlockSize += block.getTotalByteSize();
          largeBlockRecords += block.getRowCount();
          smallBlocks.add(new SmallBlock(inputFile, blockIndex,configuration, footers));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (!smallBlocks.isEmpty()) {
      blocks.add(new SmallBlocksUnion(smallBlocks, largeBlockRecords));
    }
    return unmodifiableList(blocks);
  }


  public static ParquetFileReader getReader(Path inputFile, Configuration configuration, Map<Path, ParquetMetadata> footers) throws IOException {
    ParquetMetadata footer = footers.computeIfAbsent(inputFile, (path) -> readFooter(path, configuration));
    return new ParquetFileReader(configuration, inputFile, footer);
  }

  public static ParquetMetadata readFooter(Path inputFile, Configuration configuration)  {
    try {
      return ParquetFileReader.readFooter(configuration, inputFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static class SmallBlocksUnion {
    private final List<SmallBlock> blocks;
    private final long rowCount;

    public SmallBlocksUnion(List<SmallBlock> blocks, long rowCount) {
      this.blocks = blocks;
      this.rowCount = rowCount;
    }

    public List<SmallBlock> getBlocks() {
      return blocks;
    }

    public long getRowCount() {
      return rowCount;
    }
  }

  public static class SmallBlock {
    private final Path path;
    private final int blockIndex;
    private final Configuration configuration;
    private final Map<Path, ParquetMetadata> footers;

    public SmallBlock(Path path, int blockIndex, Configuration configuration, Map<Path, ParquetMetadata> footers) {
      this.path = path;
      this.blockIndex = blockIndex;
      this.configuration = configuration;
      this.footers = footers;
    }

    public ParquetFileReader getReader() throws IOException {
      return BlocksCombiner.getReader(this.path, this.configuration, this.footers);
    }

    public int getBlockIndex() {
      return blockIndex;
    }

    public Path getPath() {
      return path;
    }
  }
}