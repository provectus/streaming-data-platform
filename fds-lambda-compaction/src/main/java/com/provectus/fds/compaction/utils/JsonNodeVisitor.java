package com.provectus.fds.compaction.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;

import java.util.Optional;

public interface JsonNodeVisitor<V> {
    Optional<V> visitObject(String name, JsonNode node);

    Optional<V> visitArray(String name, ArrayNode node);

    Optional<V> visitBinary(String name, BinaryNode binaryNode);

    Optional<V> visitText(String name, TextNode binaryNode);

    Optional<V> visitNumber(String name, NumericNode numericNode);

    Optional<V> visitBoolean(String name, BooleanNode booleanNode);

    default Optional<V> visit(String name, JsonNode node) {
        switch(node.getNodeType()) {
            case OBJECT:
                return visitObject(name, node);
            case ARRAY:
                return visitArray(name, (ArrayNode)node);
            case BINARY:
                return visitBinary(name, (BinaryNode)node);
            case STRING:
                return visitText(name, (TextNode)node);
            case NUMBER:
                return visitNumber(name, (NumericNode)node);
            case BOOLEAN:
                return visitBoolean(name, (BooleanNode)node);
            default:
                return Optional.empty();
        }
    }
}
