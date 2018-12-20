package com.provectus.fds.compaction.utils;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetTripletUtils {

  public static void consumeTriplet(ColumnWriter columnWriter, ColumnReader columnReader) {
    int definitionLevel = columnReader.getCurrentDefinitionLevel();
    int repetitionLevel = columnReader.getCurrentRepetitionLevel();
    ColumnDescriptor column = columnReader.getDescriptor();
    PrimitiveType type = column.getPrimitiveType();
    if (definitionLevel < column.getMaxDefinitionLevel()) {
      columnWriter.writeNull(repetitionLevel, definitionLevel);
    } else {
      switch (type.getPrimitiveTypeName()) {
        case INT32:
          columnWriter.write(columnReader.getInteger(), repetitionLevel, definitionLevel);
          break;
        case INT64:
          columnWriter.write(columnReader.getLong(), repetitionLevel, definitionLevel);
          break;
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
        case INT96:
          columnWriter.write(columnReader.getBinary(), repetitionLevel, definitionLevel);
          break;
        case BOOLEAN:
          columnWriter.write(columnReader.getBoolean(), repetitionLevel, definitionLevel);
          break;
        case FLOAT:
          columnWriter.write(columnReader.getFloat(), repetitionLevel, definitionLevel);
          break;
        case DOUBLE:
          columnWriter.write(columnReader.getDouble(), repetitionLevel, definitionLevel);
          break;
        default:
          throw new IllegalArgumentException("Unknown primitive type " + type);
      }
    }
    columnReader.consume();
  }
}
