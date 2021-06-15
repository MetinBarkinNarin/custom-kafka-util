package tr.com.example.kafka;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.PropertyAccessorFactory;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Created by nozdemir on 4.03.2020.
 */
public final class CustomRecordUtil {
    public static <K, V, T> SenderRecord<K, V, T> createSenderRecord(String topicName, K key, V value) {
        return SenderRecord
                .create(
                        createProducerRecord(
                                topicName,
                                key,
                                value
                        ),
                        null
                );
    }

    public static <K, V, T> SenderRecord<K, V, T> createSenderRecord(String topicName, V value) {
        return SenderRecord
                .create(
                        createProducerRecord(
                                topicName,
                                value
                        ),
                        null
                );
    }

    public static <K, V> ProducerRecord<K, V> createProducerRecord(String topicName, K key, V value) {
        return new ProducerRecord<>(
                topicName,
                key,
                value
        );
    }

    public static <K, V> ProducerRecord<K, V> createProducerRecord(String topicName, V value) {
        return new ProducerRecord<>(
                topicName,

                value
        );
    }

    public static Schema createSchema(String json) {
        return new Schema.Parser().parse(json);
    }


    public static GenericRecord createGenericRecord(Schema schema, Map<?, ?> fieldMap) {
        return schema.getFields()
                .stream()
                .reduce(new GenericData.Record(schema),
                        (record, field) -> putRecord(
                                record,
                                field.name(),
                                field.schema(),
                                fieldMap.get(field.name())
                        ),
                        (record, record2) -> record = record2
                );
    }

    private static GenericData.Record putRecord(GenericData.Record record,
                                                String name,
                                                Schema fieldSchema,
                                                Object data) {
        switch (fieldSchema.getType()) {
            case RECORD:
                data = createGenericRecord(fieldSchema, (Map<?, ?>) data);
                break;
            case ENUM:
                data = new GenericData.EnumSymbol(fieldSchema, data);
                break;
        }
        record.put(name, data);
        return record;
    }


    public static GenericRecord generateGenericRecord(Schema schemaForDomainEvent, Object object) {
        GenericData.Record record = new GenericData.Record(schemaForDomainEvent);
        schemaForDomainEvent.
                getFields().
                forEach(field -> putToGenericRecord(object, field, record));
        return record;
    }

    public static void putToGenericRecord(Object object, Schema.Field field, GenericData.Record record) {
        switch (field.schema().getType()) {

            case RECORD:
                record.put(field.name(), generateGenericRecord(field.schema(), PropertyAccessorFactory
                        .forDirectFieldAccess(object).
                                getPropertyValue(field.name())));
                break;
            case UNION:
                field.schema().getTypes()
                        .stream()
                        .filter(schema -> schema.getType() != Schema.Type.NULL)
                        .filter(schema -> Objects.equals(
                                schema.getFullName(),
                                Optional.of(object)
                                        .map(PropertyAccessorFactory::forDirectFieldAccess)
                                        .map(accessor -> accessor.getPropertyValue(field.name()))
                                        .map(Object::getClass)
                                        .map(Class::getName)
                                        .orElse("")))
                        .forEach(schemaForDomainEvent -> record.put(field.name(), generateGenericRecord(schemaForDomainEvent, PropertyAccessorFactory
                                .forDirectFieldAccess(object).
                                        getPropertyValue(field.name()))));
                break;
            case NULL:
                break;
            case ENUM:
                record.put(field.name(), new GenericData.EnumSymbol(field.schema(), Optional.of(object)
                        .map(PropertyAccessorFactory::forDirectFieldAccess)
                        .map(accessor -> accessor.getPropertyValue(field.name()))
                        .map(Object::toString)
                        .orElse("")));
                break;
            default:
                record.put(field.name(), PropertyAccessorFactory
                        .forDirectFieldAccess(object).
                                getPropertyValue(field.name()));
                break;

        }
    }

    public static Schema generateSchema(Class aClass) {
        AvroMapper objectMapper = new AvroMapper();
        CustomAvroSchemaGenerator gen = new CustomAvroSchemaGenerator();
        try {
            objectMapper.acceptJsonFormatVisitor(aClass, gen);
        } catch (JsonMappingException e) {
            e.printStackTrace();
        }
        AvroSchema avroSchema = gen.getGeneratedSchema();

        return avroSchema.getAvroSchema();
    }
}
