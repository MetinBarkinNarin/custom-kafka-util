package tr.com.example.kafka;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CustomAvroSerializer extends KafkaAvroSerializer implements Serializer<Object> {
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private boolean isKey;

    public CustomAvroSerializer() {
    }

    public CustomAvroSerializer(SchemaRegistryClient client) {
        super(client);
    }

    public CustomAvroSerializer(SchemaRegistryClient client, Map<String, ?> props) {
        super(client, props);
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        this.isKey = isKey;
        this.useSchemaReflection = true;
    }

    public byte[] serialize(String topic, Object record) {
//        Schema schema = this.getSchema(record);
//        String subjectName = this.getSubjectName(topic, this.isKey, record, schema);
        return this.serializeImpl(topic, record);
    }

    public Schema getSchema(Object record) {
        try {
            ObjectMapper objectMapper = new ObjectMapper(new AvroFactory());
            AvroSchemaGenerator gen = new AvroSchemaGenerator();
            objectMapper.acceptJsonFormatVisitor(record.getClass(), gen);
            AvroSchema generatedSchema = gen.getGeneratedSchema();

            return generatedSchema.getAvroSchema();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        }
        return Schema.create(Schema.Type.NULL);
    }

    protected byte[] serializeImpl(String topic, Object object) throws SerializationException {
        String restClientErrorMsg = "";
        Schema schema = null;
        try {
            AvroMapper objectMapper = new AvroMapper();
            CustomAvroSchemaGenerator gen = new CustomAvroSchemaGenerator();
            objectMapper.acceptJsonFormatVisitor(object.getClass(), gen);
            AvroSchema avroSchema = gen.getGeneratedSchema();
            schema = avroSchema.getAvroSchema();

            String subjectName = this.getSubjectName(topic, this.isKey, object, schema);

            int id;
            if (this.autoRegisterSchema) {
                restClientErrorMsg = "Error registering Avro schema: ";
                id = this.schemaRegistry.register(subjectName, schema);
            } else {
                restClientErrorMsg = "Error retrieving Avro schema: ";
                id = this.schemaRegistry.getId(subjectName, schema);
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(0);
            out.write(ByteBuffer.allocate(4).putInt(id).array());
            if (object instanceof byte[]) {
                out.write((byte[]) ((byte[]) object));
            } else {
                BinaryEncoder encoder = this.encoderFactory.directBinaryEncoder(out, (BinaryEncoder) null);
                Object value = object instanceof NonRecordContainer ? ((NonRecordContainer) object).getValue() : object;
                Object writer;
                if (value instanceof SpecificRecord) {
                    writer = new SpecificDatumWriter(schema);
                } else if (this.useSchemaReflection) {
                    CustomReflectData customReflectData = new CustomReflectData();
                    writer = new ReflectDatumWriter(schema, customReflectData);
                } else {
                    writer = new GenericDatumWriter(schema);
                }

                ((DatumWriter) writer).write(value, encoder);
                encoder.flush();
            }
//            AvroMapper avroMapper = new AvroMapper();
//            byte[] bytes1 = avroMapper.writer().writeValueAsBytes(object);
//            out.write(bytes1);

            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (RuntimeException | IOException var10) {
            throw new SerializationException("Error serializing Avro message", var10);
        } catch (RestClientException var11) {
            throw new SerializationException(restClientErrorMsg + schema, var11);
        }

    }
}
