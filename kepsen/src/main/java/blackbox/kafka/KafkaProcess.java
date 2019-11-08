package blackbox.kafka;

import blackbox.iface.ForkedProcess;

public class KafkaProcess extends ForkedProcess {

	public KafkaProcess(int serverId, String binaryBase, String configBase) {
		super(serverId, binaryBase, configBase);
	}

	@Override
	public ProcessBuilder createProcess() {
		return new ProcessBuilder(binaryBase + "/bin/kafka-server-start.sh", configBase + serverId);
	}
	
}