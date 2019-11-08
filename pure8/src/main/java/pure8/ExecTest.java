package pure8;

import java.io.File;
import java.io.IOException;

import org.apache.kafka.clients.producer.ProducerRecord;

import blackbox.kafka.KafkaLauncher;
import blackbox.kafka.KafkaProcess;
import blackbox.zookeeper.ZookeeperLauncher;
import blackbox.zookeeper.ZookeeperProcess;
import model.Injector;

public class ExecTest {

	public static void main(String[] args) throws InterruptedException, IOException {
		
		Injector inj = new Injector();
		ZookeeperLauncher z = new ZookeeperLauncher(1, "/home/edward/Downloads/kafka_2.11-2.2.1",
				"/home/edward/Documents/kafos/kepsen/singlenode/zookeeper.properties.",
				(i, s1, s2) -> new ZookeeperProcess(i, s1, s2));

		KafkaLauncher l = new KafkaLauncher(1, "/home/edward/Downloads/kafka_2.11-2.2.1",
				"/home/edward/Documents/kafos/kepsen/singlenode/kafkaserver.properties.",
				(i, s1, s2) -> new KafkaProcess(i, s1, s2));
		
		String boot = "localhost:9092";
		String inputTopic = "topic0";
		String outputTopic = "topic1";
		
		ExecProcessor ep= new ExecProcessor(1, "pure8.RPW", "groupa topic0 localhost:9092 topic1");
		ep.start();

		
		//new ToScreenConsuer(boot, outputTopic, "toscreen");
		new ToElasticConsuer(boot, outputTopic, "toelastic");
		new PeriodicProducer(boot, inputTopic, 1000, 0, 
				(topic, j) -> new ProducerRecord<String,String>(topic, "0", String.valueOf(j))).init();

		Thread.sleep(20000);

		

		ep.kill();
		new RecordVerifier(0, 1000, inj.kafkaConfig.repository);
		
		
		l.shutdown();
		z.shutdown();

		deleteDirectory(new File("/tmp/kafka-logs-singlenode"));
		deleteDirectory(new File("/tmp/zookeeper-singlenode"));

	}

	static void deleteDirectory(File file) throws IOException {
		if (file.isDirectory()) {
			File[] entries = file.listFiles();
			if (entries != null) {
				for (File entry : entries) {
					deleteDirectory(entry);
				}
			}
		}
		if (!file.delete()) {
			throw new IOException("Failed to delete " + file);
		}
	}

}
