package org.apache.parquet.hadoop;

import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class ColumnReadStorePublicImpl implements ColumnReadStore {
    private final PageReadStore pageReadStore;
    private final GroupConverter recordConverter;
    private final MessageType schema;
    private final VersionParser.ParsedVersion writerVersion;

    public ColumnReadStorePublicImpl(PageReadStore pageReadStore, GroupConverter recordConverter, MessageType schema, String createdBy) {
        this.pageReadStore = pageReadStore;
        this.recordConverter = recordConverter;
        this.schema = schema;

        VersionParser.ParsedVersion version;
        try {
            version = VersionParser.parse(createdBy);
        } catch (RuntimeException var7) {
            version = null;
        } catch (VersionParser.VersionParseException var8) {
            version = null;
        }

        this.writerVersion = version;
    }

    public ColumnReader getColumnReader(ColumnDescriptor path) {
        return this.newMemColumnReader(path, this.pageReadStore.getPageReader(path));
    }

    public ColumnReaderImpl newMemColumnReader(ColumnDescriptor path, PageReader pageReader) {
        PrimitiveConverter converter = this.getPrimitiveConverter(path);
        return new ColumnReaderImpl(path, pageReader, converter, this.writerVersion);
    }

    private PrimitiveConverter getPrimitiveConverter(ColumnDescriptor path) {
        Type currentType = this.schema;
        Converter currentConverter = this.recordConverter;
        String[] var4 = path.getPath();
        int var5 = var4.length;

        for(int var6 = 0; var6 < var5; ++var6) {
            String fieldName = var4[var6];
            GroupType groupType = ((Type)currentType).asGroupType();
            int fieldIndex = groupType.getFieldIndex(fieldName);
            currentType = groupType.getType(fieldName);
            currentConverter = ((Converter)currentConverter).asGroupConverter().getConverter(fieldIndex);
        }

        PrimitiveConverter converter = ((Converter)currentConverter).asPrimitiveConverter();
        return converter;
    }
}