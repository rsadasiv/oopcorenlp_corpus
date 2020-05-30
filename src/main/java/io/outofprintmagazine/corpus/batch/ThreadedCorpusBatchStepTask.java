package io.outofprintmagazine.corpus.batch;

import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class ThreadedCorpusBatchStepTask extends CorpusBatchStep implements CorpusBatchStepTask {
	
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
