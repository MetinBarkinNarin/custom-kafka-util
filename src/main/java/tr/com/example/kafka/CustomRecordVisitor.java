package tr.com.example.kafka;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitable;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.dataformat.avro.AvroFixedSize;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaHelper;
import com.fasterxml.jackson.dataformat.avro.schema.DefinedSchemas;
import com.fasterxml.jackson.dataformat.avro.schema.SchemaBuilder;
import com.fasterxml.jackson.dataformat.avro.ser.CustomEncodingSerializer;
import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroMeta;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.util.internal.JacksonUtils;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.util.*;

public class CustomRecordVisitor extends JsonObjectFormatVisitor.Base implements SchemaBuilder {
    protected final JavaType _type;
    protected final DefinedSchemas _schemas;
    protected final boolean _overridden;
    protected Schema _avroSchema;
    protected List<Schema.Field> _fields = new ArrayList();

    public CustomRecordVisitor(SerializerProvider p, JavaType type, DefinedSchemas schemas) {
        super(p);
        this._type = type;
        this._schemas = schemas;
        BeanDescription bean = this.getProvider().getConfig().introspectDirectClassAnnotations(this._type);
        List<NamedType> subTypes = this.getProvider().getAnnotationIntrospector().findSubtypes(bean.getClassInfo());
        AvroSchema ann = (AvroSchema) bean.getClassInfo().getAnnotation(AvroSchema.class);
        if (ann != null) {
            this._avroSchema = AvroSchemaHelper.parseJsonSchema(ann.value());
            this._overridden = true;
        } else if (subTypes != null && !subTypes.isEmpty()) {
            ArrayList unionSchemas = new ArrayList();

            try {
                Iterator var8 = subTypes.iterator();

                while (var8.hasNext()) {
                    NamedType subType = (NamedType) var8.next();
                    JsonSerializer<?> ser = this.getProvider().findValueSerializer(subType.getType());
                    CustomAvroSchemaGenerator visitor = new CustomAvroSchemaGenerator(this._schemas, this.getProvider());
                    ser.acceptJsonFormatVisitor(visitor, this.getProvider().getTypeFactory().constructType(subType.getType()));
                    unionSchemas.add(visitor.getAvroSchema());
                }

                this._avroSchema = Schema.createUnion(unionSchemas);
                this._overridden = true;
            } catch (JsonMappingException var12) {
                throw new RuntimeException("Failed to build schema", var12);
            }
        } else {
            this._avroSchema = AvroSchemaHelper.initializeRecordSchema(bean);
            this._overridden = false;
            AvroMeta meta = (AvroMeta) bean.getClassInfo().getAnnotation(AvroMeta.class);
            if (meta != null) {
                this._avroSchema.addProp(meta.key(), meta.value());
            }
        }

        schemas.addSchema(type, this._avroSchema);
    }

    public Schema builtAvroSchema() {
        if (!this._overridden) {
            this._avroSchema.setFields(this._fields);
        }

        return this._avroSchema;
    }

    public void property(BeanProperty writer) throws JsonMappingException {
        if (!this._overridden) {
            this._fields.add(this.schemaFieldForWriter(writer, false));
        }
    }

    public void property(String name, JsonFormatVisitable handler, JavaType type) throws JsonMappingException {
        if (!this._overridden) {
            CustomAvroSchemaGenerator wrapper = new CustomAvroSchemaGenerator(this._schemas, this.getProvider());
            handler.acceptJsonFormatVisitor(wrapper, type);
            Schema schema = wrapper.getAvroSchema();
            this._fields.add(new Schema.Field(name, schema, (String) null, (Object) null));
        }
    }

    @Override
    public void optionalProperty(BeanProperty writer) throws JsonMappingException {
        if (!this._overridden) {
            this._fields.add(this.schemaFieldForWriter(writer, true));
        }
    }

    public void optionalProperty(String name, JsonFormatVisitable handler, JavaType type) throws JsonMappingException {
        if (!this._overridden) {
            CustomAvroSchemaGenerator wrapper = new CustomAvroSchemaGenerator(this._schemas, this.getProvider());
            handler.acceptJsonFormatVisitor(wrapper, type);
            Schema schema = wrapper.getAvroSchema();
            if (!type.isPrimitive()) {
                schema = this.unionWithNull(schema);
            }

            this._fields.add(new Schema.Field(name, schema, (String) null, (Object) null));
        }
    }

    protected Schema.Field schemaFieldForWriter(BeanProperty prop, boolean optional) throws JsonMappingException {
        AvroSchema schemaOverride = (AvroSchema) prop.getAnnotation(AvroSchema.class);
        Schema writerSchema;
        if (schemaOverride != null) {
            Schema.Parser parser = new Schema.Parser();
            writerSchema = parser.parse(schemaOverride.value());
        } else {
            AvroFixedSize fixedSize = (AvroFixedSize) prop.getAnnotation(AvroFixedSize.class);
            if (fixedSize != null) {
                writerSchema = Schema.createFixed(fixedSize.typeName(), (String) null, fixedSize.typeNamespace(), fixedSize.size());
            } else {
                JsonSerializer<?> ser = null;
                if (prop instanceof BeanPropertyWriter) {
                    BeanPropertyWriter bpw = (BeanPropertyWriter) prop;
                    ser = bpw.getSerializer();
                    optional = optional && !(ser instanceof CustomEncodingSerializer);
                }

                SerializerProvider prov = this.getProvider();
                if (ser == null) {
                    if (prov == null) {
                        throw JsonMappingException.from(prov, "SerializerProvider missing for RecordVisitor");
                    }

                    ser = prov.findValueSerializer(prop.getType(), prop);
                }

                CustomAvroSchemaGenerator visitor = new CustomAvroSchemaGenerator(this._schemas, prov);
                ser.acceptJsonFormatVisitor(visitor, prop.getType());
                writerSchema = visitor.getAvroSchema();
            }
            if (optional
                    && !prop.getType().isPrimitive()
                    && !prop.getType().isCollectionLikeType()
                    && !prop.getType().isMapLikeType()
                    && prop.getType().isAbstract()) {
                writerSchema = this.unionWithNull(writerSchema);
            }
        }

        JsonNode defaultValue = this.parseJson(prop.getMetadata().getDefaultValue());
        writerSchema = this.reorderUnionToMatchDefaultType(writerSchema, defaultValue);
        Schema.Field field = new Schema.Field(prop.getName(), writerSchema, prop.getMetadata().getDescription(), JacksonUtils.toObject(defaultValue));
        AvroMeta meta = (AvroMeta) prop.getAnnotation(AvroMeta.class);
        if (meta != null) {
            field.addProp(meta.key(), meta.value());
        }

        List<PropertyName> aliases = prop.findAliases(this.getProvider().getConfig());
        if (!aliases.isEmpty()) {
            Iterator var9 = aliases.iterator();

            while (var9.hasNext()) {
                PropertyName pn = (PropertyName) var9.next();
                field.addAlias(pn.getSimpleName());
            }
        }

        return field;
    }

    private boolean isWrapper(String name) {
        return Objects.equals(name, "java.lang.Integer") ||
                Objects.equals(name, "java.lang.String") ||
                Objects.equals(name, "java.lang.Double") ||
                Objects.equals(name, "java.lang.Long") ||
                Objects.equals(name, "java.lang.Float") ||
                Objects.equals(name, "java.lang.Short") ||
                Objects.equals(name, "java.lang.Boolean") ||
                Objects.equals(name, "java.lang.Character") ||
                name.startsWith("[");

    }

    protected Schema unionWithNull(Schema otherSchema) {
        List<Schema> schemas = new ArrayList<>();
        schemas.add(Schema.create(Schema.Type.NULL));
        if (otherSchema.getType() == Schema.Type.UNION) {
            schemas.addAll(otherSchema.getTypes());
        } else {
            schemas.add(otherSchema);
        }

        return Schema.createUnion(schemas);
    }

    protected JsonNode parseJson(String defaultValue) throws JsonMappingException {
        if (defaultValue == null) {
            return null;
        } else {
            org.codehaus.jackson.map.ObjectMapper mapper = new org.codehaus.jackson.map.ObjectMapper();

            try {
                return mapper.readTree(defaultValue);
            } catch (IOException var4) {
                throw JsonMappingException.from(this.getProvider(), "Unable to parse default value as JSON: " + defaultValue, var4);
            }
        }
    }

    protected Schema reorderUnionToMatchDefaultType(Schema schema, JsonNode defaultValue) {
        if (schema != null && defaultValue != null && schema.getType() == Schema.Type.UNION) {
            List<Schema> types = new ArrayList(schema.getTypes());
            Integer matchingIndex = null;
            if (defaultValue.isArray()) {
                matchingIndex = schema.getIndexNamed(Schema.Type.ARRAY.getName());
            } else {
                int i;
                if (defaultValue.isObject()) {
                    matchingIndex = schema.getIndexNamed(Schema.Type.MAP.getName());
                    if (matchingIndex == null) {
                        for (i = 0; i < types.size(); ++i) {
                            if (((Schema) types.get(i)).getType() == Schema.Type.RECORD) {
                                matchingIndex = i;
                                break;
                            }
                        }
                    }
                } else if (defaultValue.isBoolean()) {
                    matchingIndex = schema.getIndexNamed(Schema.Type.BOOLEAN.getName());
                } else if (defaultValue.isNull()) {
                    matchingIndex = schema.getIndexNamed(Schema.Type.NULL.getName());
                } else if (defaultValue.isBinary()) {
                    matchingIndex = schema.getIndexNamed(Schema.Type.BYTES.getName());
                } else if (defaultValue.isFloatingPointNumber()) {
                    matchingIndex = schema.getIndexNamed(Schema.Type.DOUBLE.getName());
                    if (matchingIndex == null) {
                        matchingIndex = schema.getIndexNamed(Schema.Type.FLOAT.getName());
                    }
                } else if (defaultValue.isIntegralNumber()) {
                    matchingIndex = schema.getIndexNamed(Schema.Type.LONG.getName());
                    if (matchingIndex == null) {
                        matchingIndex = schema.getIndexNamed(Schema.Type.INT.getName());
                    }
                } else if (defaultValue.isTextual()) {
                    matchingIndex = schema.getIndexNamed(Schema.Type.STRING.getName());
                    if (matchingIndex == null) {
                        for (i = 0; i < types.size(); ++i) {
                            if (((Schema) types.get(i)).getType() == Schema.Type.ENUM) {
                                matchingIndex = i;
                                break;
                            }
                        }
                    }
                }
            }

            if (matchingIndex != null) {
                types.add(0, types.remove((int) matchingIndex));
                Map<String, Object> jsonProps = schema.getObjectProps();
                schema = Schema.createUnion(types);
                Iterator var6 = jsonProps.keySet().iterator();

                while (var6.hasNext()) {
                    String property = (String) var6.next();
                    schema.addProp(property, jsonProps.get(property));
                }
            }

            return schema;
        } else {
            return schema;
        }
    }


}

