package tr.com.example.kafka;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

final class TestHelper {

    static <V> void testEqual(V v, String key, Function<V, Map<String, Object>> testMethod) {
        Map<String, Object> propertiesMap = testMethod.apply(v);
        assertThat(propertiesMap)
                .isNotNull()
                .hasSize(
                        Optional.ofNullable(v)
                                .map(i -> 1)
                                .orElse(0)
                )
                .satisfies( m ->
                        assertThat(m.get(key)).isEqualTo(v)
                );
    }
}
