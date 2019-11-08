package pure8;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 
 * Generates a datum 
 *
 * @param <K> producer key
 * @param <V> producer value
 */
public interface RecordMaker<K,V> {
	/** 
	 * 
	 * @param topic for record to be written
	 * @param messageId unique id (per run)
	 * @return a producer record
	 */
	ProducerRecord<K,V> make (String topic, int messageId);
}