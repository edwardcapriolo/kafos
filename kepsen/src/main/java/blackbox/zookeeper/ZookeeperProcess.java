package blackbox.zookeeper;

import blackbox.iface.ForkedProcess;

public class ZookeeperProcess extends ForkedProcess {

	public ZookeeperProcess(int serverId, String binaryBase, String configBase) {
		super(serverId, binaryBase, configBase);
	}

	@Override
	public ProcessBuilder createProcess() {
		return new ProcessBuilder(binaryBase + "/bin/zookeeper-server-start.sh", 
				configBase + serverId);
	}
	
}