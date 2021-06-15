package tr.com.example.kafka;

public class TopicNotFoundException extends RuntimeException {
    private final String topicName;

    public TopicNotFoundException(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public String toString() {
        return "TopicNotFoundException{" +
                "topicName='" + topicName + '\'' +
                '}';
    }
}
