package blackbox.iface;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import blackbox.kafka.KafkaLauncher;

public class Launcher<T extends ForkedProcess> implements AutoCloseable {

	protected ConcurrentMap<Integer, T> processes = new ConcurrentHashMap<>();

	public Launcher(int numberOfNodes, String binaryBase, String configBase, ForkedProcessMaker<T> maker ) {
		for (int i = 0; i < numberOfNodes; i++) {
			T t = maker.make(i, binaryBase, configBase);
			processes.put(i, t);
			t.start();
		}
		
		
		Runtime.getRuntime().addShutdownHook(new Thread() 
	    { 
	      public void run() 
	      { 
	        System.out.println("Shutdown Hook is running !"); 
	        for (Entry<Integer, T> l : processes.entrySet()) {
	        	l.getValue().killHard();
	        }
	      } 
	    }); 
		
	}
	
	public ConcurrentMap<Integer, T> getProcesses(){
		return processes;
	}
	
	public void shutdown() {
		for (Entry<Integer, T> entry : processes.entrySet()) {
			entry.getValue().kill();
		}
	}
	
	@Override
	public void close() {
		for (Entry<Integer, T> entry : processes.entrySet()) {
			entry.getValue().kill();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (entry.getValue().isRunning()) {
				entry.getValue().killHard();
			}
		}
		
	}
	
	public static void main(String [] args) throws InterruptedException {
		KafkaLauncher l = new KafkaLauncher(1, "/home/edward/Downloads/kafka_2.11-2.2.1", 
				"/home/edward/Documents/kafos/kepsen/singlenode/kafkaserver.properties.", null);
		Thread.sleep(20000);
		l.shutdown();
	}


}
