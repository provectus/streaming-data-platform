package com.provectus.fds.compaction.utils;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.StorageDescriptor;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GlueConverter {
    private static GlueTypeConverter glueTypeConverter = new GlueTypeConverter();

    public static StorageDescriptor convertStorage(FileMetaData metaData, String location, StorageDescriptor insd) {
        StorageDescriptor sd = insd.clone();
        sd.setLocation(location);
        sd.setColumns(convertColumns(metaData.getSchema()));
        return sd;
    }

    public static List<Column> convertColumns(MessageType messageType) {
        return glueTypeConverter.convert(messageType)
                .stream()
                .map(GlueConverter::convertColumn)
                .collect(Collectors.toList());
    }

    public static Column convertColumn(Map.Entry<String,String> entry) {
        return new Column().withName(entry.getKey()).withType(entry.getValue());
    }
}
