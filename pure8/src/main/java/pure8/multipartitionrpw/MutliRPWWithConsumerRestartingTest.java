package pure8.multipartitionrpw;

import java.util.List;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

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

/**
 * 
 * @author edward Using 1 zk and 1 kafka that should stay running through the
 *         life of the test
 * 
 *         We start multiple RPW and kill them in an attempt to create duplicate
 *         messages.
 * 
 * 
 *         The results should look like
 * 
 *         Verification stats
 *         ------------------------------------------------------------------
 *         notFound: 0 foundCorrect: 4000 foundDuplications: 0
 *         ------------------------------------------------------------------
 *         not found list[]
 * 
 */
public class MutliRPWWithConsumerRestartingTest {

	public static void main(String[] args) throws InterruptedException, IOException {

		deleteDirectory(new File("/tmp/kafka-logs-singlenode"));
		deleteDirectory(new File("/tmp/zookeeper-singlenode"));

		Injector inj = new Injector();
		try (
			ZookeeperLauncher z = new ZookeeperLauncher(1, "/home/edward/Downloads/kafka_2.11-2.2.1",
					"/home/edward/Documents/kafos/kepsen/singlenode/zookeeper.properties.",
					(i, s1, s2) -> new ZookeeperProcess(i, s1, s2));
	
			KafkaLauncher l = new KafkaLauncher(1, "/home/edward/Downloads/kafka_2.11-2.2.1",
					"/home/edward/Documents/kafos/kepsen/singlenode/kafkaserver.properties.",
					(i, s1, s2) -> new KafkaProcess(i, s1, s2));
					){
	
			String boot = "localhost:9092";
			String inputTopic = "topic0";
			String outputTopic = "topic1";
	
			List<ExecProcessor> eps = new ArrayList<ExecProcessor>();
			for (int i = 0; i < 4; i++) {
				ExecProcessor ep = new ExecProcessor(i, "pure8.multipartitionrpw.MultiPartitionRPW",
						"groupa topic0 localhost:9092 topic1");
				ep.start();
				eps.add(ep);
			}
	
			ToElasticConsuer toElastic = new ToElasticConsuer(boot, outputTopic, "toelastic");
			new PeriodicProducer(boot, inputTopic, 4000, 15,
					(topic, j) -> new ProducerRecord<String, String>(topic, String.valueOf(j), String.valueOf(j))).init();
	
			for (int i = 0; i < 1; i++) {
				eps.get(0).killHard();
				eps.get(0).start();
				Thread.sleep(2000);
	
				eps.get(1).killHard();
				eps.get(1).start();
				Thread.sleep(2000);
	
				eps.get(3).killHard();
				eps.get(3).start();
				Thread.sleep(2000);
	
				eps.get(1).killHard();
				eps.get(1).start();
				Thread.sleep(2000);
			}
	
			Thread.sleep(80000);
	
			for (int i = 0; i < eps.size(); i++) {
				eps.get(i).kill();
			}
			new RecordVerifier(0, 4000, inj.kafkaConfig.repository);
	
			l.shutdown();
			z.shutdown();
	
			toElastic.close();
	
			deleteDirectory(new File("/tmp/kafka-logs-singlenode"));
			deleteDirectory(new File("/tmp/zookeeper-singlenode"));
			System.out.println("Hit the bottom");
			System.exit(0);
		}
	}

	static void deleteDirectory(File file) throws IOException {
		if (!file.exists()) {
			return;
		}
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
