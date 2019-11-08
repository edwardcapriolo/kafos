package blackbox.zookeeper;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import blackbox.iface.ForkedProcessMaker;
import blackbox.iface.Launcher;

public class ZookeeperLauncher extends Launcher<ZookeeperProcess>{

	public ZookeeperLauncher(int numberOfNodes, String binaryBase, String configBase,
			ForkedProcessMaker<ZookeeperProcess> maker) {
		super(numberOfNodes, binaryBase, configBase, maker);
	}

	public static void main(String [] args) throws InterruptedException {
		ZookeeperLauncher l = new ZookeeperLauncher(1, "/home/edward/Downloads/kafka_2.11-2.2.1", 
				"/home/edward/Documents/kafos/kepsen/singlenode/zookeeper.properties.", (i, s1, s2) -> new ZookeeperProcess(i, s1, s2)) ;
		Thread.sleep(20000);
		l.shutdown();
	}
}
