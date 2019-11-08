package blackbox.kafka;
import java.util.concurrent.ConcurrentMap;

class KafkaLifecylce implements Runnable {
	
	protected ConcurrentMap<Integer, KafkaProcess> processes;

	public KafkaLifecylce(ConcurrentMap<Integer, KafkaProcess> processes) {
		this.processes = processes;
	}

	public void run() {
		// kill kafka restarted it do whatever you need to do
	}
}