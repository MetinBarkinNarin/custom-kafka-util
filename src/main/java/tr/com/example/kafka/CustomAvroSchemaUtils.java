package tr.com.example.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class CustomAvroSchemaUtils {
    //
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//


    private static final Map<String, Schema> primitiveSchemas;

    public CustomAvroSchemaUtils() {
    }

    private static Schema createPrimitiveSchema(Schema.Parser parser, String type) {
        String schemaString = String.format("{\"type\" : \"%s\"}", type);
        return parser.parse(schemaString);
    }

    protected static Schema copyOf(Schema schema) {
        Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        return parser.parse(schema.toString());
    }

    protected static Map<String, Schema> getPrimitiveSchemas() {
        return Collections.unmodifiableMap(primitiveSchemas);
    }

    public static Schema getSchema(Object object) {
        return getSchema(object, false);
    }

    public static Schema getSchema(Object object, boolean useReflection) {
        if (object == null) {
            return (Schema) primitiveSchemas.get("Null");
        } else if (object instanceof Boolean) {
            return (Schema) primitiveSchemas.get("Boolean");
        } else if (object instanceof Integer) {
            return (Schema) primitiveSchemas.get("Integer");
        } else if (object instanceof Long) {
            return (Schema) primitiveSchemas.get("Long");
        } else if (object instanceof Float) {
            return (Schema) primitiveSchemas.get("Float");
        } else if (object instanceof Double) {
            return (Schema) primitiveSchemas.get("Double");
        } else if (object instanceof CharSequence) {
            return (Schema) primitiveSchemas.get("String");
        } else if (object instanceof byte[]) {
            return (Schema) primitiveSchemas.get("Bytes");
        } else if (useReflection) {
            Schema schema = CustomReflectData.get().getSchema(object.getClass());
            if (schema == null) {
                throw new SerializationException("Schema is null for object of class " + object.getClass().getCanonicalName());
            } else {
                return schema;
            }
        } else if (object instanceof GenericContainer) {
            return ((GenericContainer) object).getSchema();
        } else {
            throw new IllegalArgumentException("Unsupported Avro type. Supported types are null, Boolean, Integer, Long, Float, Double, String, byte[] and IndexedRecord");
        }
    }

    static {
        Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        primitiveSchemas = new HashMap();
        primitiveSchemas.put("Null", createPrimitiveSchema(parser, "null"));
        primitiveSchemas.put("Boolean", createPrimitiveSchema(parser, "boolean"));
        primitiveSchemas.put("Integer", createPrimitiveSchema(parser, "int"));
        primitiveSchemas.put("Long", createPrimitiveSchema(parser, "long"));
        primitiveSchemas.put("Float", createPrimitiveSchema(parser, "float"));
        primitiveSchemas.put("Double", createPrimitiveSchema(parser, "double"));
        primitiveSchemas.put("String", createPrimitiveSchema(parser, "string"));
        primitiveSchemas.put("Bytes", createPrimitiveSchema(parser, "bytes"));
    }
}

