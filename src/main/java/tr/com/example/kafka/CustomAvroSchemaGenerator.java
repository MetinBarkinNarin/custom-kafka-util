package tr.com.example.kafka;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonIntegerFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonMapFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.*;
import org.apache.avro.Schema;

public class CustomAvroSchemaGenerator extends VisitorFormatWrapperImpl {

    public CustomAvroSchemaGenerator() {
        super(new DefinedSchemas(), (SerializerProvider)null);
    }

    public AvroSchema getGeneratedSchema() {
        return new AvroSchema(this.getAvroSchema());
    }

    public CustomAvroSchemaGenerator(DefinedSchemas schemas, SerializerProvider prov) {
        super(schemas, prov);
    }

    @Override
    public JsonObjectFormatVisitor expectObjectFormat(JavaType type) {
        Schema s = this._schemas.findSchema(type);
        if (s != null) {
            this._valueSchema = s;
            return null;
        } else {
            CustomRecordVisitor v = new CustomRecordVisitor(this._provider, type, this._schemas);
            this._builder = v;
            return v;
        }
    }
    @Override
    public JsonIntegerFormatVisitor expectIntegerFormat(JavaType type) {
        Schema s = this._schemas.findSchema(type);
        if (s != null) {
            this._valueSchema = s;
            return null;
        } else {
            IntegerVisitor v = new IntegerVisitor(type);
            this._builder = v;
            return v;
        }
    }
    @Override
    public Schema getAvroSchema() {
        if (this._valueSchema != null) {
            return this._valueSchema;
        } else if (this._builder == null) {
            throw new IllegalStateException("No visit methods called on " + this.getClass().getName() + ": no schema generated");
        } else {
            return this._builder.builtAvroSchema();
        }
    }

    @Override
    public JsonMapFormatVisitor expectMapFormat(JavaType mapType) {
        CustomMapVisitor v = new CustomMapVisitor(this._provider, mapType, this._schemas);
        this._builder = v;
        return v;
    }
}
