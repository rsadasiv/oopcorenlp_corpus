package io.outofprintmagazine.corpus.batch.impl.word2vec;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.CorpusBatchStepTask;

public class Word2VecPreprocessor extends CorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(Word2VecPreprocessor.class);
	
	protected List<CorpusBatchStepTask> corpusBatchStepTasks = null;
	
	public Word2VecPreprocessor() {
		super();
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	@Override
	public ArrayNode run(ArrayNode input) {
		if (corpusBatchStepTasks == null) {
			corpusBatchStepTasks = new ArrayList<CorpusBatchStepTask>();
			if (getData().getProperties().has("tasks")) {
				ArrayNode tasks = (ArrayNode) getData().getProperties().get("tasks");
				Iterator<JsonNode> tasksIter = tasks.elements();
				while (tasksIter.hasNext()) {
					JsonNode taskNode = tasksIter.next();
					String taskName = taskNode.asText();
					try {
						
						Object task = Class.forName(
										"io.outofprintmagazine.corpus.batch.impl.word2vec." + taskName + "Preprocessor"
								).newInstance();
						CorpusBatchStep currentBatchStep = (CorpusBatchStep) task;
			    		currentBatchStep.setData(getData());
			    		corpusBatchStepTasks.add((CorpusBatchStepTask) task);
					}
					catch (Exception e) {
						logger.error(e);
					}
				}
			}
		}
		return super.run(input);
	}

	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		
		if (corpusBatchStepTasks != null) {
			for (CorpusBatchStepTask task : corpusBatchStepTasks) {
				task.enrichOne(outputStepItem);
			}
		}
        
		retval.add(outputStepItem);
		return retval;
	}
}
