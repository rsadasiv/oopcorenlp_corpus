package io.outofprintmagazine.corpus.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class ThreadedCorpusBatchStep extends CorpusBatchStep implements ICorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(ThreadedCorpusBatchStep.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}

	public ThreadedCorpusBatchStep() {
		super();
	}

	@Override
	public ArrayNode runOne(ObjectNode input) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	protected int maxThreads = 50;
	protected String taskClass = "io.outofprintmagazine.corpus.batch.impl.PostgreSQLCoreNLPLoader";
	protected List<Thread> threads = new ArrayList<Thread>();
	protected Map<ObjectNode, ThreadedCorpusBatchStepTask> tasks = new HashMap<ObjectNode, ThreadedCorpusBatchStepTask>();
	
	
	public ArrayNode run(ArrayNode input) {
		//getLogger().debug("run");
		maxThreads = getData().getProperties().get("maxThreads").asInt();
		taskClass = getData().getProperties().get("taskClass").asText();
		push(input);
		execute();
		return pop(input);
	}

	protected void execute() {
		int currentIdx = 0;
		while (currentIdx<threads.size()) {
			for (int i=0;i<maxThreads&&i+currentIdx<threads.size();i++) {
				threads.get(currentIdx+i).start();
				//getLogger().debug("starting: " + (i+currentIdx));
			}
			for (int i=0;i<maxThreads&&i+currentIdx<threads.size();i++) {
				try {
					threads.get(currentIdx+i).join();
				}
				catch (Exception e) {
					getLogger().error(e);
				}
			}
			//getLogger().debug("joined");
			currentIdx=currentIdx+maxThreads;
		}
	}
	
	protected void push(ArrayNode input) {
		int count = 0;
		for (JsonNode inputItem : input) {
			if (getData().getProperties().has("maxInput") && getData().getProperties().get("maxInput").asInt() < count ) {
				break;
			}
			count++;
			boolean foundInputItem = false;

			if (!(getData().getProperties().has("noCache") && getData().getProperties().get("noCache").asBoolean())) {
				for (JsonNode existingInputItem : getData().getInput()) {
					if (existingInputItem.equals(inputItem)) {
						foundInputItem = true;
						break;
					}
				}
			}
			if (!foundInputItem) {
				try {

					if (!(getData().getProperties().has("noCache") && getData().getProperties().get("noCache").asBoolean())) {
						getData().getInput().add(inputItem);
					}
					//getLogger().debug("pushing: " + getDocID((ObjectNode)(inputItem)));
					Object task = Class.forName(taskClass).getConstructor().newInstance();
					ThreadedCorpusBatchStepTask currentBatchStep = (ThreadedCorpusBatchStepTask) task;
					currentBatchStep.setData(getData());
					currentBatchStep.setStorage(getStorage());
					currentBatchStep.setParameterStore(getParameterStore());
					currentBatchStep.setInput((ObjectNode)inputItem);
					tasks.put((ObjectNode)inputItem, currentBatchStep);
					threads.add(new Thread(currentBatchStep, getDocID((ObjectNode)inputItem)));
					
				}
				catch (Throwable t) {
					t.printStackTrace();
					getLogger().error(t);
				}
			}
		}

	}
	
	public ArrayNode pop(ArrayNode input) {
		for (JsonNode inputItem : input) {
			if (tasks.get((ObjectNode)inputItem) != null) {
				try {
	
					ArrayNode generatedOutput = tasks.get((ObjectNode)inputItem).getOutput();
					if (generatedOutput == null) {
						getLogger().debug("no generated output for: " + getDocID((ObjectNode)inputItem));
					}
					else {
						for (JsonNode generatedOutputItem : generatedOutput) {
							getData().getOutput().add(generatedOutputItem);
						}
					}
				}
				catch (Throwable t) {
					t.printStackTrace();
					getLogger().error(t);
				}
			}
		}
		return getData().getOutput();
	}

}
