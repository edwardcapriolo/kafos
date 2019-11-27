package pure8;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface ConsumerCreator<K,V> {
	KafkaConsumer<K,V> createKafkaConsumer(String boot, String group);
}