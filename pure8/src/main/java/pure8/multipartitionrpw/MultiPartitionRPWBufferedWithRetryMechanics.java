package pure8.multipartitionrpw;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

class RetryableException extends RuntimeException{

	public RetryableException(String message, Throwable cause) {
		super(message, cause);
	}

	public RetryableException(Throwable cause) {
		super(cause);
	}

	private static final long serialVersionUID = 1L;
	
}

interface RetryableMessageProcessor<K,V> {
	List<ProducerRecord<K,V>> process(ConsumerRecord<K,V> record);
}

class TrackingProducerWithRetryMechanics<K,V> {
	private final TopicPartition partitionToTrack;
	private final KafkaProducer<K,V> kafkaProducer;
	private final List<ProducerRecord<K,V>> buffer = new ArrayList<>();
	
	public TrackingProducerWithRetryMechanics(KafkaProducer<K,V> kafkaProducer, TopicPartition partitionToTrack) {
		this.partitionToTrack = partitionToTrack;
		this.kafkaProducer = kafkaProducer;
		kafkaProducer.initTransactions();
	}
	
	public void send(List<ProducerRecord<K,V>> messages){
		buffer.addAll(messages);
	}
	
	public void commitAndClear(ConsumerRecords<K,V> records, String groupId) {
		if (buffer.size() == 0) {
			return;
		}
		
		try {
			kafkaProducer.beginTransaction();
			for (ProducerRecord<K, V> l : buffer) {
				kafkaProducer.send(l);
			}
			Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
			for (TopicPartition partition : records.partitions()) {
				if (partition.partition() == partitionToTrack.partition()) {
					List<ConsumerRecord<K,V>> partitionedRecords= records.records(partition);
					long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
					offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
				}
			}
			kafkaProducer.sendOffsetsToTransaction(offsetsToCommit, groupId);
			kafkaProducer.commitTransaction();
		} catch (KafkaException e) {
			kafkaProducer.abortTransaction();
		} finally {
			buffer.clear();
		}
	}
}

public class MultiPartitionRPWBufferedWithRetryMechanics<K,V> implements  Runnable {

	protected final String groupId;
	protected final String inputTopic;
	protected final String bootstrap;
	protected final AtomicBoolean goOn = new AtomicBoolean(true);
	protected final ConcurrentMap<Integer, BufferedTrackingProducer<K,V>> producers= new ConcurrentHashMap<>();
	protected final RetryableMessageProcessor<K,V> processor;
	protected final ProducerCreator<K,V> producerCreator;
	protected final ConsumerCreator<K,V> consumerCreator;
	
	protected KafkaConsumer<K, V> consumer;
	
	public MultiPartitionRPWBufferedWithRetryMechanics(String groupId, String inputTopic, String bootstrap,
			RetryableMessageProcessor<K,V> processor,
			ProducerCreator<K,V> producerCreator,
			ConsumerCreator<K,V> consumerCreator){
		this.groupId = groupId;
		this.inputTopic = inputTopic;
		this.bootstrap = bootstrap;
		this.processor = processor;
		this.producerCreator = producerCreator;
		this.consumerCreator = consumerCreator;
	}
	
	public void runOnce() {
		ConsumerRecords<K,V> records = null;
		try { 
			records = consumer.poll(2000);
		} catch (KafkaException e){
			return;
		}
		
		for(ConsumerRecord<K,V> record: records) {
			int partition = record.partition();
			BufferedTrackingProducer<K,V> producer = producers.get(partition);
			int i=0;
			List<ProducerRecord<K, V>> results = null;
			do {
				try {
					results = processor.process(record);
					break;
				} catch (RetryableException e) {}
			} while(i< 10);
			producer.send(results);

		}
		for (Entry<Integer, BufferedTrackingProducer<K, V>> entry: producers.entrySet()) {
			entry.getValue().commitAndClear(records, groupId);	
		}
	
	}
	
	public void init() {
		consumer = consumerCreator.createKafkaConsumer(bootstrap, groupId);
		consumer.subscribe(Arrays.asList(inputTopic), new ConsumerRebalanceListener() {

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> topics) {
				for (TopicPartition topic: topics) {
					if(!producers.containsKey(topic.partition())) {
						String fenceId = inputTopic + "-" + topic.partition();
						producers.put (topic.partition(), new BufferedTrackingProducer<K,V>(producerCreator.createKafkaProducer(bootstrap, fenceId),topic));
					}
				}
				
			}

			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> arg0) {
				
			}}  );
		Thread t = new Thread(this);
		t.start();
	}
	
	public void run() {
		while (goOn.get()) {
			runOnce();
		}
	}

	public static void main(String []args) {
		String [] split = args[0].split("\\s+");
		RetryableMessageProcessor<String,String> mp = new RetryableMessageProcessor<String,String>() {

			@Override
			public List<ProducerRecord<String, String>> process(ConsumerRecord<String, String> record) {
				return Arrays.asList(new ProducerRecord<String,String>(split[3], record.key(), record.value()));
			}
			
		};

		MultiPartitionRPWBufferedWithRetryMechanics<String,String> r = new MultiPartitionRPWBufferedWithRetryMechanics<String,String> (split[0], split[1], split[2], 
				mp, SharedUtils.prod, SharedUtils.con);
		r.init();
	}
	
}
