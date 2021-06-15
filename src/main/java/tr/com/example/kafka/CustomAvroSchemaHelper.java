package tr.com.example.kafka;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedConstructor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatTypes;
import com.fasterxml.jackson.databind.util.ClassUtil;
import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroAlias;
import org.apache.avro.reflect.Stringable;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.util.*;

public class CustomAvroSchemaHelper {
    public static final String AVRO_SCHEMA_PROP_CLASS = "java-class";
    public static final String AVRO_SCHEMA_PROP_KEY_CLASS = "java-key-class";
    public static final String AVRO_SCHEMA_PROP_ELEMENT_CLASS = "java-element-class";
    protected static final Set<Class<?>> STRINGABLE_CLASSES = new HashSet(Arrays.asList(URI.class, URL.class, File.class, BigInteger.class, BigDecimal.class, String.class,Character.class));

    public CustomAvroSchemaHelper() {
    }

    public static boolean isStringable(AnnotatedClass type) {
        if (!STRINGABLE_CLASSES.contains(type.getRawType()) && !Enum.class.isAssignableFrom(type.getRawType())) {
            if (type.getAnnotated().getAnnotation(Stringable.class) == null) {
                return false;
            } else {
                Iterator var1 = type.getConstructors().iterator();

                AnnotatedConstructor constructor;
                do {
                    if (!var1.hasNext()) {
                        return false;
                    }

                    constructor = (AnnotatedConstructor)var1.next();
                } while(constructor.getParameterCount() != 1 || constructor.getRawParameterType(0) != String.class);

                return true;
            }
        } else {
            return true;
        }
    }

    protected static String getNamespace(JavaType type) {
        Class<?> cls = type.getRawClass();
        Class<?> enclosing = cls.getEnclosingClass();
        if (enclosing != null) {
            return enclosing.getName() + "$";
        } else {
            Package pkg = cls.getPackage();
            return pkg == null ? "" : pkg.getName();
        }
    }

    protected static String getName(JavaType type) {
        String name;
        for(name = type.getRawClass().getSimpleName(); name.indexOf("[]") >= 0; name = name.replace("[]", "Array")) {
        }

        return name;
    }

    protected static Schema unionWithNull(Schema otherSchema) {
        List<Schema> schemas = new ArrayList();
        schemas.add(Schema.create(Schema.Type.NULL));
        if (otherSchema.getType() == Schema.Type.UNION) {
            schemas.addAll(otherSchema.getTypes());
        } else {
            schemas.add(otherSchema);
        }

        return Schema.createUnion(schemas);
    }

    public static Schema simpleSchema(JsonFormatTypes type, JavaType hint) {
        switch(type) {
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case INTEGER:
                return Schema.create(Schema.Type.INT);
            case NULL:
                return Schema.create(Schema.Type.NULL);
            case NUMBER:
                if (hint.hasRawClass(Float.TYPE)) {
                    return Schema.create(Schema.Type.FLOAT);
                } else {
                    if (hint.hasRawClass(Long.TYPE)) {
                        return Schema.create(Schema.Type.LONG);
                    }

                    return Schema.create(Schema.Type.DOUBLE);
                }
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case ARRAY:
            case OBJECT:
                throw new UnsupportedOperationException("Should not try to create simple Schema for: " + type);
            case ANY:
            default:
                throw new UnsupportedOperationException("Can not create Schema for: " + type + "; not (yet) supported");
        }
    }

    public static Schema numericAvroSchema(JsonParser.NumberType type) {
        switch(type) {
            case INT:
                return Schema.create(Schema.Type.INT);
            case LONG:
                return Schema.create(Schema.Type.LONG);
            case FLOAT:
                return Schema.create(Schema.Type.FLOAT);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case BIG_INTEGER:
            case BIG_DECIMAL:
                return Schema.create(Schema.Type.STRING);
            default:
                throw new IllegalStateException("Unrecognized number type: " + type);
        }
    }

    public static Schema numericAvroSchema(JsonParser.NumberType type, JavaType hint) {
        Schema schema = numericAvroSchema(type);
        if (hint != null) {
            schema.addProp("java-class", getTypeId(hint));
        }

        return schema;
    }

    public static Schema typedSchema(Schema.Type nativeType, JavaType javaType) {
        Schema schema = Schema.create(nativeType);
        schema.addProp("java-class", getTypeId(javaType));
        return schema;
    }

    public static Schema anyNumberSchema() {
        return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.DOUBLE)));
    }

    public static Schema stringableKeyMapSchema(JavaType mapType, JavaType keyType, Schema valueSchema) {
        Schema schema = Schema.createMap(valueSchema);
        if (mapType != null && !mapType.hasRawClass(Map.class)) {
            schema.addProp("java-class", getTypeId(mapType));
        }

        if (keyType != null && !keyType.hasRawClass(String.class)) {
            schema.addProp("java-key-class", getTypeId(keyType));
        }

        return schema;
    }

    protected static <T> T throwUnsupported() {
        throw new UnsupportedOperationException("Format variation not supported");
    }

    public static Schema initializeRecordSchema(BeanDescription bean) {
        return addAlias(Schema.createRecord(getName(bean.getType()), bean.findClassDescription(), getNamespace(bean.getType()), bean.getType().isTypeOrSubTypeOf(Throwable.class)), bean);
    }

    public static Schema parseJsonSchema(String json) {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(json);
    }

    public static Schema createEnumSchema(BeanDescription bean, List<String> values) {
        return addAlias(Schema.createEnum(getName(bean.getType()), bean.findClassDescription(), getNamespace(bean.getType()), values), bean);
    }

    public static Schema addAlias(Schema schema, BeanDescription bean) {
        AvroAlias ann = (AvroAlias)bean.getClassInfo().getAnnotation(AvroAlias.class);
        if (ann != null) {
            schema.addAlias(ann.alias(), ann.space().equals("NOT A VALID NAMESPACE") ? null : ann.space());
        }

        return schema;
    }

    public static String getTypeId(JavaType type) {
        return getTypeId(type.getRawClass());
    }

    public static String getTypeId(Class<?> type) {
        return type.isPrimitive() ? ClassUtil.wrapperType(type).getName() : type.getName();
    }

    public static String getTypeId(Schema schema) {
        switch(schema.getType()) {
            case RECORD:
            case ENUM:
            case FIXED:
                return getFullName(schema);
            default:
                return schema.getProp("java-class");
        }
    }

    public static String getFullName(Schema schema) {
        switch(schema.getType()) {
            case RECORD:
            case ENUM:
            case FIXED:
                String namespace = schema.getNamespace();
                String name = schema.getName();
                if (namespace == null) {
                    return schema.getName();
                } else {
                    if (namespace.endsWith("$")) {
                        return namespace + name;
                    }

                    StringBuilder sb = new StringBuilder(1 + namespace.length() + name.length());
                    return sb.append(namespace).append('.').append(name).toString();
                }
            default:
                return schema.getType().getName();
        }
    }
}
