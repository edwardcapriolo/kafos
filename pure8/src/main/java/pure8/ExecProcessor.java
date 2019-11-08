package pure8;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import blackbox.iface.ForkedProcess;

public class ExecProcessor extends ForkedProcess {
	
	  private static final Logger LOGGER = LogManager.getLogger(ExecProcessor.class);

	/*
	 * According to the docs you should be able to do this
	 * 'a' 'b' 'c=d'
	 * but I have not had luck with that so the main has to split the args
	 */
	String args;
	String mainClass;
	public ExecProcessor(int serverId, String mainClass, String args) {
		super(serverId, null, null);
		this.args = args;
		this.mainClass = mainClass;
	}

	@Override
	public ProcessBuilder createProcess() {
		System.err.println(mainClass +" "+ args);
		return new ProcessBuilder("mvn", "exec:java", "-Dexec.mainClass="+ mainClass, "-Dexec.args=\"" +args+"\"" );
	}

}
