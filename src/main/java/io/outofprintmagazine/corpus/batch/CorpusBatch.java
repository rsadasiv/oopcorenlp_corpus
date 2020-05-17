package io.outofprintmagazine.corpus.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.outofprintmagazine.corpus.batch.db.CorpusBatchModel;
import io.outofprintmagazine.corpus.batch.db.CorpusBatchStepModel;
import io.outofprintmagazine.corpus.storage.BatchStorage;
import io.outofprintmagazine.corpus.storage.ScratchStorage;


public class CorpusBatch {
	
	private static final Logger logger = LogManager.getLogger(CorpusBatch.class);
	
	public CorpusBatch() {
		super();
	}
	
	private CorpusBatchModel data;
	
	public CorpusBatchModel getData() {
		return data;
	}

	public void setData(CorpusBatchModel data) {
		this.data = data;
	}
	
	private ScratchStorage scratchStorage = null;
	
	public ScratchStorage getScratchStorage() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (scratchStorage == null) {
			scratchStorage = (ScratchStorage) Class.forName(getData().getScratchStorageClass()).newInstance();
		}
		return scratchStorage;
	}
	
	public void setScratchStorage(ScratchStorage storage) {
		this.scratchStorage = storage;
	}
	
	private BatchStorage batchStorage = null;
	
	public BatchStorage getBatchStorage() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if (batchStorage == null) {
			batchStorage = (BatchStorage) Class.forName(getData().getBatchStorageClass()).newInstance();
		}
		return batchStorage;
	}
	
	public void setBatchStorage(BatchStorage batchStorage) {
		this.batchStorage = batchStorage;
	}
	
	public static CorpusBatch buildFromTemplate(String templateLocation) throws IOException {
    	CorpusBatch corpusBatch = new CorpusBatch();
    	ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    	corpusBatch.setData(
    			mapper.readValue(
    					corpusBatch.getClass().getClassLoader().getResourceAsStream(templateLocation),
    					CorpusBatchModel.class
    			)
    	);
    	return corpusBatch;
	}
	
	public static CorpusBatch buildFromStagingBatch(String corpusName, String batchName) throws Exception {
		return buildFromStagingBatch(corpusName, batchName, "io.outofprintmagazine.corpus.storage.file.FileCorpora");
	}
	
	public static CorpusBatch buildFromStagingBatch(String corpusName, String batchName, String batchStorageClass) throws Exception {
    	BatchStorage batchStorage = (BatchStorage) Class.forName(batchStorageClass).newInstance();
    	CorpusBatch corpusBatch = new CorpusBatch();
    	corpusBatch.setBatchStorage(batchStorage);
    	ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    	corpusBatch.setData(
    			mapper.treeToValue(
    					batchStorage.getStagingBatch(corpusName, batchName), 
    					CorpusBatchModel.class
    			)
    	);
    	return corpusBatch;
	}
	
	public static CorpusBatch buildFromJson(String corpusName, JsonNode data) throws Exception {
    	CorpusBatch corpusBatch = new CorpusBatch();
    	ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    	corpusBatch.setData(
    			mapper.treeToValue(
    					data, 
    					CorpusBatchModel.class
    			)
    	);
    	return corpusBatch;
	}
	
	public static CorpusBatch buildFromString(String corpusName, String data) throws Exception {
    	CorpusBatch corpusBatch = new CorpusBatch();
    	ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    	corpusBatch.setData(
    			mapper.readValue(
    					data, 
    					CorpusBatchModel.class
    			)
    	);
    	return corpusBatch;
	}
	
	public void run() throws Exception {
		List<CorpusBatchStepModel> sortedSteps = new ArrayList<CorpusBatchStepModel>(getData().getCorpusBatchSteps());
		Collections.sort(sortedSteps);
		ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		CorpusBatchStep previousBatchStep = null;
		CorpusBatchStep currentBatchStep = null;
    	for (CorpusBatchStepModel corpusBatchStepModel : sortedSteps) {
    		previousBatchStep = currentBatchStep;
    		currentBatchStep = (CorpusBatchStep) Class.forName(corpusBatchStepModel.getCorpusBatchStepClass()).newInstance();
    		currentBatchStep.setData(corpusBatchStepModel);
    		currentBatchStep.setStorage(getScratchStorage());
    		currentBatchStep.getData().setCorpusId(getData().getCorpusId());
    		currentBatchStep.getData().setCorpusBatchId(getData().getCorpusBatchId());
    		if (previousBatchStep != null) {
    			logger.debug(getData().getCorpusId() + " " + getData().getCorpusBatchId() + " " + currentBatchStep.getData().getCorpusBatchStepId());
    	    	currentBatchStep.run(previousBatchStep.getData().getOutput());
    		}
    		else {
    			currentBatchStep.run(currentBatchStep.getData().getInput().deepCopy());
    		}
    	}
    	
    	getBatchStorage().storeStagingBatchString(getData().getCorpusId(), getData().getCorpusBatchId(), mapper.writeValueAsString(getData()));
	}
	
	public void runStep(String stepId) throws Exception {
		List<CorpusBatchStepModel> sortedSteps = new ArrayList<CorpusBatchStepModel>(getData().getCorpusBatchSteps());
		Collections.sort(sortedSteps);
		ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		CorpusBatchStep previousBatchStep = null;
		CorpusBatchStep currentBatchStep = null;
    	for (CorpusBatchStepModel corpusBatchStepModel : sortedSteps) {
    		previousBatchStep = currentBatchStep;
    		currentBatchStep = (CorpusBatchStep) Class.forName(corpusBatchStepModel.getCorpusBatchStepClass()).newInstance();
    		currentBatchStep.setData(corpusBatchStepModel);
    		if (corpusBatchStepModel.getCorpusBatchStepId().equals(stepId)) {
	    		if (previousBatchStep != null) {
	    			logger.debug(getData().getCorpusId() + " " + getData().getCorpusBatchId() + " " + currentBatchStep.getData().getCorpusBatchStepId());
	    	    	currentBatchStep.run(previousBatchStep.getData().getOutput());
	    		}
	    		else {
	    			currentBatchStep.run(currentBatchStep.getData().getInput());
	    		}
    		}
    	}
    	getBatchStorage().storeStagingBatchString(getData().getCorpusId(), getData().getCorpusBatchId(), mapper.writeValueAsString(getData()));
	}

}
