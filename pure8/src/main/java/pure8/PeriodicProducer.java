package pure8;

import org.apache.kafka.clients.producer.KafkaProducer;

import pure8.rpw.RPW;

/**
 * Produces messages with a random delay thread ends after sending numberOfMessages
 *
 */
public class PeriodicProducer implements Runnable{

	private final String boot;
	private final String topic;
	private final long delayInMillis;
	private final int numberOfMessages;
	private final RecordMaker maker;
	
	public PeriodicProducer(String boot, String topic, int numberOfMessages, int delayInMillis, RecordMaker maker) {
		this.boot = boot;
		this.topic = topic;
		this.delayInMillis = delayInMillis;
		this.numberOfMessages = numberOfMessages;
		this.maker = maker;
	}
	
	public void init() {
		new Thread(this).start();
	}
	
	public void run() {
		KafkaProducer<String, String> producer = RPW.createKafkaProducer(boot);
		
		for (int i = 0; i < numberOfMessages; i++) {
			while(true) {
				try {
					producer.send( maker.make(topic, i));
					break;
				} catch (RuntimeException ex) {
					System.err.println(ex);
				}
			} 
				
			try {
				Thread.sleep(delayInMillis);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		producer.close();
	}
}
