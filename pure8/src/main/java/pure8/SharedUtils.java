package pure8;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class SharedUtils {
	
	/*
	 *
	 * produce a transaction.id based completely on the group, topic, and partition.
	 * You do not want anything random or host based in here because then fence will not work correctly.
	 */
	public static String makeFencingId(String topic, String group, int partitionId) {
		return topic + "." + group + "." + partitionId;
	}
	
	public static ProducerCreator<String,String> prod = new ProducerCreator<String,String>() {
		public KafkaProducer<String,String> createKafkaProducer(String boot, String transId){
			Map<String,Object> properties = new HashMap<>();
			properties.put("bootstrap.servers", boot);
			properties.put("transactional.id", transId);
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			return new KafkaProducer<String,String>(properties);
		}
	};
	 
	public static  ConsumerCreator<String,String> con = new ConsumerCreator<String,String>(){
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
}
