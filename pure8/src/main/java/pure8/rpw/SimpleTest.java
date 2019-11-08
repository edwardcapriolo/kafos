package pure8.rpw;

import java.io.File;
import java.io.IOException;

import org.apache.kafka.clients.producer.ProducerRecord;

import blackbox.kafka.KafkaLauncher;
import blackbox.kafka.KafkaProcess;
import blackbox.zookeeper.ZookeeperLauncher;
import blackbox.zookeeper.ZookeeperProcess;
import model.Injector;
import pure8.ExecProcessor;
import pure8.PeriodicProducer;
import pure8.RecordVerifier;
import pure8.ToElasticConsuer;

public class SimpleTest {

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
		
		RPW r = new RPW("groupa", inputTopic, boot, outputTopic);
		r.start();
		
		ExecProcessor ep= new ExecProcessor(1, "pure8.rpw.RPW", "");
		ep.start();
		
		//new ToScreenConsuer(boot, outputTopic, "toscreen");
		new ToElasticConsuer(boot, outputTopic, "toelastic");
		new PeriodicProducer(boot, inputTopic, 1000, 0, 
				(topic, j) -> new ProducerRecord<String,String>(topic, "0", String.valueOf(j))).init();

		Thread.sleep(20000);
		new RecordVerifier(0, 1000, inj.kafkaConfig.repository);
		
		r.stop();
		ep.kill();
		
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
