package tr.com.example.kafka;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

public class CustomReflectData extends ReflectData {

    public int resolveUnion(Schema union, Object datum) {
        List<Schema> types = union.getTypes();

        return IntStream
                .range(0, types.size())
                .filter(i -> types.get(i) != null && Objects.equals(types.get(i).getFullName(), datum.getClass().getName()))
                .findFirst()
                .orElse(0);
    }

}
