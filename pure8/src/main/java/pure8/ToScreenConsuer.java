package pure8;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import pure8.rpw.RPW;


/**
 * 
 * this prints records and dies when the vm dies
 *
 */
public class ToScreenConsuer {

	public ToScreenConsuer(String boot, String topic, String group) {
		KafkaConsumer<String, String> consumer = RPW.createKafkaConsumer(boot, group);
		consumer.subscribe(Arrays.asList(topic));
		Thread t = new Thread( () -> { 
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record);
			}
		}
		});
		t.setDaemon(true);
		t.start();
	}
}
