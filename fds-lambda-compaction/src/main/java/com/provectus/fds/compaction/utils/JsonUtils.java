package com.provectus.fds.compaction.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class JsonUtils {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Optional<Schema> buildSchema(File file) throws IOException {
        Schema schema = null;

        try(
                FileReader reader = new FileReader(file);
                BufferedReader bufferedReader = new BufferedReader(reader)
        ) {
            String currentLine;
            SchemaNodeVisitor schemaNodeVisitor = new SchemaNodeVisitor();
            while((currentLine=bufferedReader.readLine()) != null) {
                JsonNode jsonRecord = mapper.readTree(currentLine);
                Optional<Schema.Field> field = schemaNodeVisitor.visit("record", jsonRecord);
                if (field.isPresent()) {
                    if (schema == null) {
                        schema = field.get().schema();
                    } else {
                        schema = SchemaUtils.merge(schema, field.get().schema());
                    }
                }
            }
        }

        return Optional.ofNullable(schema);
    }

    public static Object convertToAvro(GenericData model, JsonNode datum, Schema schema) {
        if (datum == null) {
            return null;
        }
        switch (schema.getType()) {
            case RECORD:
                Object record = model.newRecord(null, schema);
                for (Schema.Field field : schema.getFields()) {
                    model.setField(record, field.name(), field.pos(),
                            convertField(model, datum.get(field.name()), field));
                }
                return record;

            case MAP:
                Map<String, Object> map = Maps.newLinkedHashMap();
                Iterator<Map.Entry<String, JsonNode>> iter = datum.fields();
                while (iter.hasNext()) {
                    Map.Entry<String, JsonNode> entry = iter.next();
                    map.put(entry.getKey(), convertToAvro(
                            model, entry.getValue(), schema.getValueType()));
                }
                return map;

            case ARRAY:
                List<Object> list = Lists.newArrayListWithExpectedSize(datum.size());
                for (JsonNode element : datum) {
                    list.add(convertToAvro(model, element, schema.getElementType()));
                }
                return list;

            case UNION:
                return convertToAvro(model, datum,
                        resolveUnion(datum, schema.getTypes()));

            case BOOLEAN:
                return datum.booleanValue();

            case FLOAT:
                return datum.floatValue();

            case DOUBLE:
                return datum.doubleValue();

            case INT:
                return datum.intValue();

            case LONG:
                return datum.longValue();

            case STRING:
                return datum.textValue();

            case ENUM:
                return model.createEnum(datum.textValue(), schema);

            case BYTES:
                try {
                    return ByteBuffer.wrap(datum.binaryValue());
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read JSON binary", e);
                }

            case FIXED:
                byte[] bytes;
                try {
                    bytes = datum.binaryValue();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read JSON binary", e);
                }
                return model.createFixed(null, bytes, schema);

            case NULL:
                return null;

            default:
                // don't use DatasetRecordException because this is a Schema problem
                throw new IllegalArgumentException("Unknown schema type: " + schema);
        }
    }

    private static Object convertField(GenericData model, JsonNode datum,
                                       Schema.Field field) {
        try {
            Object value = convertToAvro(model, datum, field.schema());
            if (value != null || SchemaUtils.nullOk(field.schema())) {
                return value;
            } else {
                return model.getDefaultValue(field);
            }
        } catch (AvroRuntimeException e) {
            throw new RuntimeException(String.format(
                    "Field %s: cannot make %s value: '%s'",
                    field.name(), field.schema(), String.valueOf(datum)), e);
        }
    }

    private static Schema resolveUnion(JsonNode datum, Collection<Schema> schemas) {
        Set<Schema.Type> primitives = Sets.newHashSet();
        List<Schema> others = Lists.newArrayList();
        for (Schema schema : schemas) {
            if (PRIMITIVES.containsKey(schema.getType())) {
                primitives.add(schema.getType());
            } else {
                others.add(schema);
            }
        }

        // Try to identify specific primitive types
        Schema primitiveSchema = null;
        if (datum == null || datum.isNull()) {
            primitiveSchema = closestPrimitive(primitives, Schema.Type.NULL);
        } else if (datum.isShort() || datum.isInt()) {
            primitiveSchema = closestPrimitive(primitives,
                    Schema.Type.INT, Schema.Type.LONG,
                    Schema.Type.FLOAT, Schema.Type.DOUBLE);
        } else if (datum.isLong()) {
            primitiveSchema = closestPrimitive(primitives,
                    Schema.Type.LONG, Schema.Type.DOUBLE);
        } else if (datum.isFloat()) {
            primitiveSchema = closestPrimitive(primitives,
                    Schema.Type.FLOAT, Schema.Type.DOUBLE);
        } else if (datum.isDouble()) {
            primitiveSchema = closestPrimitive(primitives, Schema.Type.DOUBLE);
        } else if (datum.isBoolean()) {
            primitiveSchema = closestPrimitive(primitives, Schema.Type.BOOLEAN);
        }

        if (primitiveSchema != null) {
            return primitiveSchema;
        }

        // otherwise, select the first schema that matches the datum
        for (Schema schema : others) {
            if (matches(datum, schema)) {
                return schema;
            }
        }

        throw new RuntimeException(String.format(
                "Cannot resolve union: %s not in %s", datum, schemas));
    }

    // this does not contain string, bytes, or fixed because the datum type
    // doesn't necessarily determine the schema.
    private static ImmutableMap<Schema.Type, Schema> PRIMITIVES = ImmutableMap
            .<Schema.Type, Schema>builder()
            .put(Schema.Type.NULL, Schema.create(Schema.Type.NULL))
            .put(Schema.Type.BOOLEAN, Schema.create(Schema.Type.BOOLEAN))
            .put(Schema.Type.INT, Schema.create(Schema.Type.INT))
            .put(Schema.Type.LONG, Schema.create(Schema.Type.LONG))
            .put(Schema.Type.FLOAT, Schema.create(Schema.Type.FLOAT))
            .put(Schema.Type.DOUBLE, Schema.create(Schema.Type.DOUBLE))
            .build();

    private static Schema closestPrimitive(Set<Schema.Type> possible, Schema.Type... types) {
        for (Schema.Type type : types) {
            if (possible.contains(type) && PRIMITIVES.containsKey(type)) {
                return PRIMITIVES.get(type);
            }
        }
        return null;
    }

    private static boolean matches(JsonNode datum, Schema schema) {
        switch (schema.getType()) {
            case RECORD:
                if (datum.isObject()) {
                    // check that each field is present or has a default
                    boolean missingField = false;
                    for (Schema.Field field : schema.getFields()) {
                        if (!datum.has(field.name()) && field.defaultValue() == null) {
                            missingField = true;
                            break;
                        }
                    }
                    if (!missingField) {
                        return true;
                    }
                }
                break;
            case UNION:
                if (resolveUnion(datum, schema.getTypes()) != null) {
                    return true;
                }
                break;
            case MAP:
                if (datum.isObject()) {
                    return true;
                }
                break;
            case ARRAY:
                if (datum.isArray()) {
                    return true;
                }
                break;
            case BOOLEAN:
                if (datum.isBoolean()) {
                    return true;
                }
                break;
            case FLOAT:
                if (datum.isFloat() || datum.isInt()) {
                    return true;
                }
                break;
            case DOUBLE:
                if (datum.isDouble() || datum.isFloat() ||
                        datum.isLong() || datum.isInt()) {
                    return true;
                }
                break;
            case INT:
                if (datum.isInt()) {
                    return true;
                }
                break;
            case LONG:
                if (datum.isLong() || datum.isInt()) {
                    return true;
                }
                break;
            case STRING:
                if (datum.isTextual()) {
                    return true;
                }
                break;
            case ENUM:
                if (datum.isTextual() && schema.hasEnumSymbol(datum.textValue())) {
                    return true;
                }
                break;
            case BYTES:
            case FIXED:
                if (datum.isBinary()) {
                    return true;
                }
                break;
            case NULL:
                if (datum == null || datum.isNull()) {
                    return true;
                }
                break;
            default: // UNION or unknown
                throw new IllegalArgumentException("Unsupported schema: " + schema);
        }
        return false;
    }

}
