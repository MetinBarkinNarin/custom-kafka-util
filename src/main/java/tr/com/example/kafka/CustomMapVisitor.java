package tr.com.example.kafka;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitable;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonMapFormatVisitor;
import com.fasterxml.jackson.dataformat.avro.schema.DefinedSchemas;
import com.fasterxml.jackson.dataformat.avro.schema.SchemaBuilder;
import com.fasterxml.jackson.dataformat.avro.schema.VisitorFormatWrapperImpl;
import org.apache.avro.Schema;

public class CustomMapVisitor extends JsonMapFormatVisitor.Base implements SchemaBuilder {
    protected final JavaType _type;
    protected final DefinedSchemas _schemas;
    protected Schema _valueSchema;
    protected JavaType _keyType;

    public CustomMapVisitor(SerializerProvider p, JavaType type, DefinedSchemas schemas) {
        super(p);
        this._type = type;
        this._schemas = schemas;
    }

    public Schema builtAvroSchema() {
        if (this._valueSchema == null) {
            throw new IllegalStateException("Missing value type for " + this._type);
        } else {
            AnnotatedClass ac = this._provider.getConfig().introspectClassAnnotations(this._keyType).getClassInfo();
            if (CustomAvroSchemaHelper.isStringable(ac)) {
                return CustomAvroSchemaHelper.stringableKeyMapSchema(this._type, this._keyType, this._valueSchema);
            } else {
                throw new UnsupportedOperationException("Maps with non-stringable keys are not supported yet");
            }
        }
    }

    public void keyFormat(JsonFormatVisitable handler, JavaType keyType) throws JsonMappingException {
        this._keyType = keyType;
    }

    public void valueFormat(JsonFormatVisitable handler, JavaType valueType) throws JsonMappingException {
        VisitorFormatWrapperImpl wrapper = new VisitorFormatWrapperImpl(this._schemas, this.getProvider());
        handler.acceptJsonFormatVisitor(wrapper, valueType);
        this._valueSchema = wrapper.getAvroSchema();
    }
}
