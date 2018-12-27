package com.provectus.fds.compaction.utils;

import org.apache.parquet.schema.*;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.Type.Repetition.REPEATED;

public class GlueTypeConverter {

    private static final boolean ADD_LIST_ELEMENT_RECORDS_DEFAULT = true;

    public List<Map.Entry<String,String>> convert(MessageType parquetSchema) {
        List<Map.Entry<String,String>> result = new ArrayList<>();
        for (Type type : parquetSchema.getFields()) {
            result.add(new AbstractMap.SimpleEntry<>(type.getName(), convertField(type)));
        }
        return result;
    }

    public String convert(GroupType parquetSchema) {
        return convertFields(parquetSchema.getName(), parquetSchema.getFields());
    }

    private String convertFields(String name, List<Type> parquetFields) {
        StringBuilder sb = new StringBuilder();
        sb.append("struct<");
        boolean first = true;
        for (Type parquetType : parquetFields) {
            String fieldSchema = convertField(parquetType);
            if (parquetType.isRepetition(REPEATED)) {
                throw new UnsupportedOperationException("REPEATED not supported outside LIST or MAP. Type: " + parquetType);
            } else {
                if (first) {
                    first = false;
                } else {
                    sb.append(", ");
                }

                sb.append(parquetType.getName());
                sb.append(":");
                sb.append(fieldSchema);
            }
        }
        sb.append(">");
        return sb.toString();
    }

    private String convertField(final Type parquetType) {
        if (parquetType.isPrimitive()) {
            final PrimitiveType asPrimitive = parquetType.asPrimitiveType();
            final PrimitiveType.PrimitiveTypeName parquetPrimitiveTypeName =
                    asPrimitive.getPrimitiveTypeName();

            final OriginalType annotation = parquetType.getOriginalType();
            return parquetPrimitiveTypeName.convert(
                    new PrimitiveType.PrimitiveTypeNameConverter<String, RuntimeException>() {

                        @Override
                        public String convertFLOAT(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            return "float";
                        }

                        @Override
                        public String convertDOUBLE(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            return "double";
                        }

                        @Override
                        public String convertINT32(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            return "int";
                        }

                        @Override
                        public String convertINT64(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            return "bigint";
                        }

                        @Override
                        public String convertINT96(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            return "string";
                        }

                        @Override
                        public String convertFIXED_LEN_BYTE_ARRAY(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            int size = parquetType.asPrimitiveType().getTypeLength();
                            if (annotation == OriginalType.UTF8 || annotation == OriginalType.ENUM) {
                                return "char("+size+")";
                            } else {
                                return "binary";
                            }
                        }

                        @Override
                        public String convertBOOLEAN(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            return "boolean";
                        }

                        @Override
                        public String convertBINARY(PrimitiveType.PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                            if (annotation == OriginalType.UTF8 || annotation == OriginalType.ENUM) {
                                return "string";
                            } else {
                                return "binary";
                            }
                        }
                    }
            );

        } else {
            GroupType parquetGroupType = parquetType.asGroupType();
            OriginalType originalType = parquetGroupType.getOriginalType();
            if (originalType != null) {
                switch(originalType) {
                    case LIST:
                        if (parquetGroupType.getFieldCount()!= 1) {
                            throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
                        }

                        Type repeatedType = parquetGroupType.getType(0);
                        if (!repeatedType.isRepetition(REPEATED)) {
                            throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
                        }
                        if (isElementType(repeatedType, parquetGroupType.getName())) {
                            // repeated element types are always required
                            return String.format("array<%s>", convertField(repeatedType));
                        } else {
                            return String.format("array<%s>", convertField(repeatedType));
                        }

                    case MAP_KEY_VALUE: // for backward-compatibility
                    case MAP:
                        if (parquetGroupType.getFieldCount() != 1 || parquetGroupType.getType(0).isPrimitive()) {
                            throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
                        }
                        GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
                        if (!mapKeyValType.isRepetition(REPEATED) ||
                                mapKeyValType.getFieldCount()!=2) {
                            throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
                        }
                        Type keyType = mapKeyValType.getType(0);
                        if (!keyType.isPrimitive() ||
                                !keyType.asPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.BINARY) ||
                                !keyType.getOriginalType().equals(OriginalType.UTF8)) {
                            throw new IllegalArgumentException("Map key type must be binary (UTF8): "
                                    + keyType);
                        }
                        Type valueType = mapKeyValType.getType(1);
                        return String.format("map<%s,%s>", convertField(keyType), convertField(valueType));
                    case ENUM:
                        return "string";
                    case UTF8:
                    default:
                        throw new UnsupportedOperationException("Cannot convert Parquet type " +
                                parquetType);

                }
            } else {
                // if no original type then it's a record
                return convertFields(parquetGroupType.getName(), parquetGroupType.getFields());
            }
        }
    }


    private boolean isElementType(Type repeatedType, String parentName) {
        return (
                // can't be a synthetic layer because it would be invalid
                repeatedType.isPrimitive() ||
                        repeatedType.asGroupType().getFieldCount() > 1 ||
                        repeatedType.asGroupType().getType(0).isRepetition(REPEATED) ||
                        // known patterns without the synthetic layer
                        repeatedType.getName().equals("array") ||
                        repeatedType.getName().equals(parentName + "_tuple") ||
                        // default assumption
                        ADD_LIST_ELEMENT_RECORDS_DEFAULT
        );
    }
}
