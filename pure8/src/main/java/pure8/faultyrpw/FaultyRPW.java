package pure8.faultyrpw;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class FaultyRPW {

	private final String groupId;
	private final String inputTopic;
	private final String bootstrap;
	private final String outputTopic;
	private final AtomicBoolean goOn = new AtomicBoolean(true);
	
	public FaultyRPW (String groupId, String inputTopic, String bootstrap, String outputTopic){
		this.groupId = groupId;
		this.inputTopic = inputTopic;
		this.bootstrap = bootstrap;
		this.outputTopic = outputTopic;
	}
	
	public void start() {
		KafkaProducer<String,String> producer = createKafkaProducer(bootstrap);
		KafkaConsumer<String,String> consumer = createKafkaConsumer(bootstrap, groupId);			
		consumer.subscribe(Arrays.asList(inputTopic));
		Thread t = new Thread( () -> {
			while (goOn.get()) {
				try {
					ConsumerRecords<String, String> records = consumer.poll(2000);
					if (records == null || records.isEmpty()) {
						continue;
					}
					String lastValue = null;
					for (ConsumerRecord<String, String> record : records) {
						producer.send(new ProducerRecord<String, String>(outputTopic, record.key(), record.value()));
						lastValue = record.value();
					}
					System.err.println("FaultyRPW processed last value in poll: " + lastValue);
				} catch (KafkaException e) {
					//System.err.println(e);
				}
			} });
		t.start();
	}
	
	
	
	public static KafkaConsumer<String,String> createKafkaConsumer(String boot, String group){
		Map<String,Object> properties = new HashMap<>();
		properties.put("bootstrap.servers", boot);
		properties.put("group.id", group);
		properties.put("auto.offset.reset", "earliest");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return new KafkaConsumer<String,String>(properties);
	}
	
	
	public static KafkaProducer<String,String> createKafkaProducer(String boot){
		Map<String,Object> properties = new HashMap<>();
		properties.put("bootstrap.servers", boot);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new KafkaProducer<String,String>(properties);
	}
	
	public static void main(String []args) {
		String [] split = args[0].split("\\s+");
		FaultyRPW r = new FaultyRPW(split[0], split[1], split[2], split[3]);
		r.start();
	}
	
}
