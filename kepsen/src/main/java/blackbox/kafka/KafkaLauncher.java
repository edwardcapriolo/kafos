package blackbox.kafka;
import blackbox.iface.*;
import blackbox.iface.Launcher;

public class KafkaLauncher extends Launcher<KafkaProcess>{

	public KafkaLauncher(int numberOfNodes, String binaryBase, String configBase,
			ForkedProcessMaker<KafkaProcess> maker) {
		super(numberOfNodes, binaryBase, configBase, maker);
	}

	public static void main(String [] args) throws InterruptedException {
		KafkaLauncher l = new KafkaLauncher(1, "/home/edward/Downloads/kafka_2.11-2.2.1", 
				"/home/edward/Documents/kafos/kepsen/singlenode/kafkaserver.properties.", 
				(i, s1, s2) -> new KafkaProcess(i, s2, s1));
		Thread.sleep(20000);
		l.shutdown();
	}
}
