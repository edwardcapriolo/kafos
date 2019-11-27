package pure8;

import org.apache.kafka.clients.producer.KafkaProducer;

public interface ProducerCreator<K,V> {
	KafkaProducer<K,V> createKafkaProducer(String boot, String transId);
}