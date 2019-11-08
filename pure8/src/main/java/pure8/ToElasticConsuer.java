package pure8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import model.Injector;
import model.KafkaRecord;
import pure8.rpw.RPW;

public class ToElasticConsuer {

	public void cleanIt(Injector i) {
		i.kafkaConfig.operations.deleteIndex(KafkaRecord.class);
		i.kafkaConfig.operations.createIndex(KafkaRecord.class);
		i.kafkaConfig.operations.refresh(KafkaRecord.class);
	}
	
	Injector i;
	public ToElasticConsuer(String boot, String topic, String group) {
		i = new Injector();
		
		cleanIt(i);
		KafkaConsumer<String, String> consumer = RPW.createKafkaConsumer(boot, group);
		//List<PartitionInfo> partitions = consumer.partitionsFor(topic);
		consumer.subscribe(Arrays.asList(topic));
		//TODO switch to assign here to get all partitions
		
		Thread t = new Thread( () -> { 
			while(true) {
				String lastValue = null;
				ConsumerRecords<String, String> records = consumer.poll(2000);
				if (records.isEmpty()) {
					continue;
				}
				List<KafkaRecord> krs = new ArrayList<>();
				for (ConsumerRecord<String, String> record : records) {
					KafkaRecord kr = new KafkaRecord();
					kr.key = record.key();
					kr.offset = record.offset();
					kr.partition = (long) record.partition();
					kr.value = record.value();
					lastValue = kr.value;
					krs.add(kr);
					try {
						//i.kafkaConfig.repository.save(kr);
						lastValue = kr.value;
					} catch (Exception ex) {
						System.err.println(ex);
					}
				}
				i.kafkaConfig.repository.saveAll(krs);
				krs.clear();
				System.err.println("ToElastic "+ lastValue);
			}
			
			});
		t.setDaemon(true);
		t.start();
		
	}
	
	public void close() {
		i.ctx.close();
	}
}