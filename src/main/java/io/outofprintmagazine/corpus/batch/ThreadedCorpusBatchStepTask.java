package io.outofprintmagazine.corpus.batch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class ThreadedCorpusBatchStepTask extends CorpusBatchStep implements ICorpusBatchStepTask {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ThreadedCorpusBatchStepTask.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
	
	protected ObjectNode input = null;
	protected ArrayNode output = null;
	
	@Override
	public void run() {
		try {
			output = runOne(input);
		} 
		catch (Exception e) {
			getLogger().error(e);
		}
	}

	@Override
	public void setInput(ObjectNode outputStepItem) throws Exception {
		input = outputStepItem;
		
	}

	@Override
	public ArrayNode getOutput() {
		return output;
	}

}
