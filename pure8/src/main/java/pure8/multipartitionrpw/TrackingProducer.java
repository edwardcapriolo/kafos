package pure8.multipartitionrpw;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

class TrackingProducer <K,V>{

	private final TopicPartition partitionToTrack;
	private final KafkaProducer<K,V> kafkaProducer;
	private final List<ProducerRecord<K,V>> buffer = new ArrayList<>();
	
	public TrackingProducer(KafkaProducer<K,V> kafkaProducer, TopicPartition partitionToTrack) {
		this.partitionToTrack = partitionToTrack;
		this.kafkaProducer = kafkaProducer;
		kafkaProducer.initTransactions();
	}
	
	public void send(List<ProducerRecord<K,V>> messages){
		buffer.addAll(messages);

	}
	
	public void commitAndClear(ConsumerRecords<K,V> records,String groupId) {
		System.err.println("beginning batch "+ buffer.size() + buffer);
		if(buffer.isEmpty()) {
			return;
		}
		try {
			kafkaProducer.beginTransaction();
			if (Math.random() < .1) {
				System.err.println("Random exception");
				throw new KafkaException("Throwing a random exception just to ruin your day");
			}
			for (ProducerRecord<K, V> l: buffer) {
				kafkaProducer.send(l);
			}
			Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
			for (TopicPartition partition : records.partitions()) {
				if (partition.partition() == partitionToTrack.partition()) {
					List<ConsumerRecord<K,V>> partitionedRecords = records.records(partition);
					long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
					offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
				}
			}
			kafkaProducer.sendOffsetsToTransaction(offsetsToCommit, groupId);
			kafkaProducer.commitTransaction();
		} catch (KafkaException e) {
			System.err.println("aboring TX" + e);
			kafkaProducer.abortTransaction();
			throw e;
		} finally {
			System.err.println("batch complete "+ buffer);
			buffer.clear();
		}
	}
	
	public void close() {
		kafkaProducer.close();
	}
}