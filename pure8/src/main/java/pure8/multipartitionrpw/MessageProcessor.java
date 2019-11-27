package pure8.multipartitionrpw;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

interface MessageProcessor<K,V> {
	List<ProducerRecord<K,V>> process(ConsumerRecord<K,V> record);
}