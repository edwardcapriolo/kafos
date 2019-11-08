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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/*
interface MessageProcessorCallback<K,V> {
	void onCompletion(RecordMetadata m, Exception e, ConsumerRecord<K,V> originalRecord, ProducerRecord<K,V> problemOutputRecord);
}

class MessageProcessorResponse<K,V> {
	public MessageProcessorResponse(ConsumerRecord<K,V> originalRecord, 
			List<ProducerRecord<K,V>> output, MessageProcessorCallback<K,V> callback) {

		
	}
}
*/
interface MessageProcessor<K,V> {
	List<ProducerRecord<K,V>> process(ConsumerRecord<K,V> record);
}

interface ProducerCreator<K,V> {
	KafkaProducer<K,V> createKafkaProducer(String boot, String transId);
}

interface ConsumerCreator<K,V> {
	KafkaConsumer<K,V> createKafkaConsumer(String boot, String group);
}

class TrackingProducer <K,V>{
	private final AtomicBoolean used = new AtomicBoolean(false);
	private final TopicPartition partitionToTrack;
	private final KafkaProducer<K,V> kafkaProducer;
	
	public TrackingProducer(KafkaProducer<K,V> kafkaProducer, TopicPartition partitionToTrack) {
		this.partitionToTrack = partitionToTrack;
		this.kafkaProducer = kafkaProducer;
		kafkaProducer.initTransactions();
	}
	
	public List<Future<RecordMetadata>> send(List<ProducerRecord<K,V>> messages){
		if (!used.get()) {
			kafkaProducer.beginTransaction();
		}
		
		used.set(true);
		List<Future<RecordMetadata>> results = new ArrayList<>(messages.size());
		for (ProducerRecord<K, V> message : messages) {
			results.add(kafkaProducer.send(message));
		}
		return results;
	}
	
	public void commitAndClear(ConsumerRecords<K,V> records,String groupId) {
		if (!used.get()) {
			return;
		}
		try {
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
			used.set(false);
		}
	}
}

public class MultiPartitionRPW<K,V> implements  Runnable {

	protected final String groupId;
	protected final String inputTopic;
	protected final String bootstrap;
	protected final AtomicBoolean goOn = new AtomicBoolean(true);
	protected final ConcurrentMap<Integer, TrackingProducer<K,V>> producers= new ConcurrentHashMap<>();
	protected final MessageProcessor<K,V> processor;
	protected final ProducerCreator<K,V> producerCreator;
	protected final ConsumerCreator<K,V> consumerCreator;
	
	protected KafkaConsumer<K, V> consumer;
	
	public MultiPartitionRPW(String groupId, String inputTopic, String bootstrap,
			MessageProcessor<K,V> processor,
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
			TrackingProducer<K,V> producer = producers.get(partition);
			List<ProducerRecord<K, V>> results = processor.process(record);
			producer.send(results);

		}
		for (Entry<Integer, TrackingProducer<K, V>> entry: producers.entrySet()) {
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
						producers.put (topic.partition(), new TrackingProducer<K,V>(producerCreator.createKafkaProducer(bootstrap, fenceId),topic));
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
		MessageProcessor<String,String> mp = new MessageProcessor<String,String>() {

			@Override
			public List<ProducerRecord<String, String>> process(ConsumerRecord<String, String> record) {
				return Arrays.asList(new ProducerRecord<String,String>(split[3], record.key(), record.value()));
			}
			
		};
		ProducerCreator<String,String> prod = new ProducerCreator<String,String>() {
			public KafkaProducer<String,String> createKafkaProducer(String boot, String transId){
				Map<String,Object> properties = new HashMap<>();
				properties.put("bootstrap.servers", boot);
				properties.put("transactional.id", transId);
				properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
				properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
				return new KafkaProducer<String,String>(properties);
			}
		};
		
		ConsumerCreator<String,String> con = new ConsumerCreator<String,String>(){
			public KafkaConsumer<String,String> createKafkaConsumer(String boot, String group){
				Map<String,Object> properties = new HashMap<>();
				properties.put("bootstrap.servers", boot);
				properties.put("group.id", group);
				properties.put("isolation.level", "read_committed");
				properties.put("enable.auto.commit", "false");
				properties.put("auto.offset.reset", "earliest");
				properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
				return new KafkaConsumer<String,String>(properties);
			}
		};
		MultiPartitionRPW<String,String> r = new MultiPartitionRPW<String,String> (split[0], split[1], split[2], 
				mp, 
				prod,
				con);
		r.init();
	}
	
}
