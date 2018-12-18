package com.provectus.fds.compaction.utils;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class SchemaUtils {
    private static final Schema NULL = Schema.create(Schema.Type.NULL);
    private static final NullNode NULL_DEFAULT = NullNode.getInstance();
    private static float SIMILARITY_THRESH = 0.3f;



    private static Schema unwrapNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION && schema.getTypes().size() == 2) {
            List<Schema> types = schema.getTypes();
            if (types.get(0).getType() == Schema.Type.NULL) {
                return types.get(1);
            }

            if (types.get(1).getType() == Schema.Type.NULL) {
                return types.get(0);
            }
        }

        return schema;
    }

    public static Schema merge(Iterable<Schema> schemas) {
        Iterator<Schema> iter = schemas.iterator();
        if (!iter.hasNext()) {
            return null;
        }
        Schema result = iter.next();
        while (iter.hasNext()) {
            result = merge(result, iter.next());
        }
        return result;
    }

    public static Schema mergeOrUnion(Iterable<Schema> schemas) {
        Iterator<Schema> iter = schemas.iterator();
        if (!iter.hasNext()) {
            return null;
        }
        Schema result = iter.next();
        while (iter.hasNext()) {
            result = mergeOrUnion(result, iter.next());
        }
        return result;
    }

    public static Schema merge(Schema left, Schema right) {
        Schema merged = mergeOnly(left, right);
        return merged;
    }

    private static Schema mergeOrUnion(Schema left, Schema right) {
        Schema merged = mergeOnly(left, right);
        return merged != null ? merged : union(left, right);
    }

    private static Schema union(Schema left, Schema right) {
        if (left.getType() != Schema.Type.UNION) {
            return right.getType() == Schema.Type.UNION ? union(right, left) : Schema.createUnion(ImmutableList.of(left, right));
        } else if (right.getType() == Schema.Type.UNION) {
            Schema combined = left;

            Schema type;
            for(Iterator i$ = right.getTypes().iterator(); i$.hasNext(); combined = union(combined, type)) {
                type = (Schema)i$.next();
            }

            return combined;
        } else {
            boolean notMerged = true;
            List<Schema> types = Lists.newArrayList();
            Iterator<Schema> schemas = left.getTypes().iterator();

            while(schemas.hasNext()) {
                Schema next = schemas.next();
                Schema merged = mergeOnly(next, right);
                if (merged != null) {
                    types.add(merged);
                    notMerged = false;
                    break;
                }

                types.add(next);
            }

            while(schemas.hasNext()) {
                types.add(schemas.next());
            }

            if (notMerged) {
                types.add(right);
            }

            return Schema.createUnion(types);
        }
    }

    private static Schema mergeOnly(Schema left, Schema right) {
        if (Objects.equal(left, right)) {
            return left;
        } else {
            switch(left.getType()) {
                case INT:
                    if (right.getType() == Schema.Type.LONG) {
                        return right;
                    }
                    break;
                case LONG:
                    if (right.getType() == Schema.Type.INT) {
                        return left;
                    }
                    break;
                case FLOAT:
                    if (right.getType() == Schema.Type.DOUBLE) {
                        return right;
                    }
                    break;
                case DOUBLE:
                    if (right.getType() == Schema.Type.FLOAT) {
                        return left;
                    }
            }

            if (left.getType() != right.getType()) {
                return null;
            } else {
                switch(left.getType()) {
                    case RECORD:
                        if (left.getName() == null && right.getName() == null && fieldSimilarity(left, right) < SIMILARITY_THRESH) {
                            return null;
                        } else {
                            if (!Objects.equal(left.getName(), right.getName())) {
                                return null;
                            }

                            Schema combinedRecord = Schema.createRecord(coalesce(left.getName(), right.getName()), coalesce(left.getDoc(), right.getDoc()), coalesce(left.getNamespace(), right.getNamespace()), false);
                            combinedRecord.setFields(mergeFields(left, right));
                            return combinedRecord;
                        }
                    case UNION:
                        return union(left, right);
                    case ARRAY:
                        return Schema.createArray(mergeOrUnion(left.getElementType(), right.getElementType()));
                    case MAP:
                        return Schema.createMap(mergeOrUnion(left.getValueType(), right.getValueType()));
                    case BOOLEAN:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case STRING:
                    default:
                        throw new UnsupportedOperationException("Unknown schema type: " + left.getType());
                    case ENUM:
                        if (!Objects.equal(left.getName(), right.getName())) {
                            return null;
                        } else {
                            Set<String> symbols = Sets.newLinkedHashSet();
                            symbols.addAll(left.getEnumSymbols());
                            symbols.addAll(right.getEnumSymbols());
                            return Schema.createEnum(left.getName(), coalesce(left.getDoc(), right.getDoc()), coalesce(left.getNamespace(), right.getNamespace()), ImmutableList.copyOf(symbols));
                        }
                }
            }
        }
    }

    private static Schema nullableForDefault(Schema schema) {
        if (schema.getType() == Schema.Type.NULL) {
            return schema;
        } else if (schema.getType() != Schema.Type.UNION) {
            return Schema.createUnion(ImmutableList.of(NULL, schema));
        } else if (schema.getTypes().get(0).getType() == Schema.Type.NULL) {
            return schema;
        } else {
            List<Schema> types = Lists.newArrayList();
            types.add(NULL);

            for (Schema type : schema.getTypes()) {
                if (type.getType() != Schema.Type.NULL) {
                    types.add(type);
                }
            }

            return Schema.createUnion(types);
        }
    }

    private static List<Schema.Field> mergeFields(Schema left, Schema right) {
        List<Schema.Field> fields = Lists.newArrayList();
        for (Schema.Field leftField : left.getFields()) {
            Schema.Field rightField = right.getField(leftField.name());
            if (rightField != null) {
                fields.add(new Schema.Field(
                        leftField.name(),
                        mergeOrUnion(leftField.schema(), rightField.schema()),
                        coalesce(leftField.doc(), rightField.doc()),
                        coalesce(leftField.defaultValue(), rightField.defaultValue())
                ));
            } else {
                if (leftField.defaultValue() != null) {
                    fields.add(copy(leftField));
                } else {
                    fields.add(new Schema.Field(
                            leftField.name(), nullableForDefault(leftField.schema()),
                            leftField.doc(), NULL_DEFAULT
                    ));
                }
            }
        }

        for (Schema.Field rightField : right.getFields()) {
            if (left.getField(rightField.name()) == null) {
                if (rightField.defaultValue() != null) {
                    fields.add(copy(rightField));
                } else {
                    fields.add(new Schema.Field(
                            rightField.name(), nullableForDefault(rightField.schema()),
                            rightField.doc(), NULL_DEFAULT
                    ));
                }
            }
        }

        return fields;
    }

    public static Schema.Field copy(Schema.Field field) {
        return new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue());
    }

    private static float fieldSimilarity(Schema left, Schema right) {
        Set<String> leftNames = names(left.getFields());
        Set<String> rightNames = names(right.getFields());
        int common = Sets.intersection(leftNames, rightNames).size();
        float leftRatio = (float)common / (float)leftNames.size();
        float rightRatio = (float)common / (float)rightNames.size();
        return hmean(leftRatio, rightRatio);
    }

    private static Set<String> names(Collection<Schema.Field> fields) {
        Set<String> names = Sets.newHashSet();
        for (Schema.Field field : fields) {
            names.add(field.name());
        }

        return names;
    }

    private static float hmean(float left, float right) {
        return 2.0F * left * right / (left + right);
    }

    private static <E> E coalesce(E... objects) {
        for(int i = 0; i < objects.length; ++i) {
            E object = objects[i];
            if (object != null) {
                return object;
            }
        }
        return null;
    }

    public static boolean nullOk(Schema schema) {
        if (Schema.Type.NULL == schema.getType()) {
            return true;
        } else {
            if (Schema.Type.UNION == schema.getType()) {
                for (Schema possible : schema.getTypes()) {
                    if (nullOk(possible)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
