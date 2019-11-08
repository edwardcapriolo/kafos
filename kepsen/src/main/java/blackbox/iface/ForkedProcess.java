package blackbox.iface;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public abstract class ForkedProcess{
	protected String binaryBase;
	protected String configBase;
	protected int serverId;
	protected Process process;
	
	public ForkedProcess(int serverId, String binaryBase, String configBase){
		this.binaryBase = binaryBase;
		this.configBase = configBase;
		this.serverId = serverId;
	}
	
	public abstract ProcessBuilder createProcess();
	public void start() {
		try {
			process = createProcess().start();
			//process = new ProcessBuilder(binaryBase + "/bin/kafka-server-start.sh", 
			//		configBase +serverId).start();
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		Thread out = new Thread( () ->{
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line;
			try {
				while ((line = br.readLine()) != null) {
				  System.out.println("serverid: " + serverId +" out line " + line);
				}
			} catch (IOException e) {
				System.err.println("serverid: " + serverId +" out line " + e.getMessage()); 
			}
		});
		out.setDaemon(true);
		out.start();
		
		Thread error = new Thread( () ->{
			InputStream is = process.getErrorStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line;
			try {
				while ((line = br.readLine()) != null) {
					System.out.println("serverid: "+serverId +" out line " + line);
				}
			} catch (IOException e) {
				System.err.println("serverid: "+serverId +" out line " + e.getMessage());
			}
		});
		error.setDaemon(true);
		error.start();
		
	}
	
	public boolean isRunning() {
		return process.isAlive();
	}
	
	public void kill() {
		process.destroy();
	}
	
	public void killHard() {
		process.destroyForcibly();
	}
}