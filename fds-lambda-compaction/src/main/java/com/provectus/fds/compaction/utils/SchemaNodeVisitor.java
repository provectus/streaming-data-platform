package com.provectus.fds.compaction.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import org.apache.avro.Schema;

import java.util.*;

public class SchemaNodeVisitor implements JsonNodeVisitor<Schema.Field> {
    public Optional<Schema.Field> visitObject(String name, JsonNode node) {
        List<Schema.Field> fields = new ArrayList<>();
        Iterator<Map.Entry<String,JsonNode>> fieldsIterator = node.fields();
        while (fieldsIterator.hasNext()) {
            Map.Entry<String,JsonNode> field = fieldsIterator.next();
            visit(field.getKey(), field.getValue()).ifPresent(fields::add);
        }
        return Optional.of(new Schema.Field(name, Schema.createRecord(name,"","", false, fields), "", null));
    }

    public Optional<Schema.Field> visitArray(String name, ArrayNode node) {
        if (node.size()>0) {
            Optional<Schema.Field> arrayField = visit("test", node.get(0));
            if (arrayField.isPresent()) {
                return Optional.of(new Schema.Field(name, Schema.createArray(arrayField.get().schema()), "", null));
            }
        }
        return Optional.empty();
    }

    public Optional<Schema.Field> visitBinary(String name, BinaryNode binaryNode) {
        Schema schema = Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.BYTES)
        );

        return Optional.of(new Schema.Field(name, schema, "", null));
    }

    public Optional<Schema.Field> visitText(String name, TextNode binaryNode) {
        Schema schema = Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.STRING)
        );

        return Optional.of(new Schema.Field(name, schema, "", null));
    }


    public Optional<Schema.Field> visitNumber(String name, NumericNode numericNode) {
        Schema schema = Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.LONG)
        );

        return Optional.of(new Schema.Field(name, schema, "", null));
    }

    public Optional<Schema.Field> visitBoolean(String name, BooleanNode booleanNode) {
        Schema schema = Schema.createUnion(
                Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.BOOLEAN)
        );

        return Optional.of(new Schema.Field(name, schema, "", null));
    }
}
