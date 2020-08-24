/*******************************************************************************
 * Copyright (C) 2020 Ram Sadasiv
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package io.outofprintmagazine.corpus.batch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.outofprintmagazine.corpus.batch.model.CorpusBatchModel;
import io.outofprintmagazine.corpus.batch.model.CorpusBatchStepModel;
import io.outofprintmagazine.corpus.storage.IBatchStorage;
import io.outofprintmagazine.corpus.storage.IScratchStorage;
import io.outofprintmagazine.util.IParameterStore;


public class CorpusBatch {
	
	private static final Logger logger = LogManager.getLogger(CorpusBatch.class);
	
	private Logger getLogger() {
		return logger;
	}
	
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
	
	private IScratchStorage scratchStorage = null;
	
	public IScratchStorage getScratchStorage() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
		if (scratchStorage == null) {
			scratchStorage = (IScratchStorage) Class.forName(getData().getScratchStorageClass()).newInstance();
			scratchStorage.setParameterStore(getParameterStore());
		}
		return scratchStorage;
	}
	
	public void setScratchStorage(IScratchStorage storage) {
		this.scratchStorage = storage;
	}
	
	private IBatchStorage batchStorage = null;
	
	public IBatchStorage getBatchStorage() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
		if (batchStorage == null) {
			batchStorage = (IBatchStorage) Class.forName(getData().getBatchStorageClass()).newInstance();
			batchStorage.setParameterStore(getParameterStore());
		}
		return batchStorage;
	}
	
	public void setBatchStorage(IBatchStorage batchStorage) {
		this.batchStorage = batchStorage;
	}
	
	private IParameterStore parameterStore = null;
	
	public IParameterStore getParameterStore() throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException  {
		if (parameterStore == null) {
			parameterStore = (IParameterStore) Class.forName(getData().getParameterStoreClass()).newInstance();
			parameterStore.init(getData().getProperties());
		}
		return parameterStore;
	}
	
	public void setBatchStorage(IParameterStore parameterStore) {
		this.parameterStore = parameterStore;
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
    	IBatchStorage batchStorage = (IBatchStorage) Class.forName(batchStorageClass).newInstance();
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
	
	public static CorpusBatch buildFromFile(String corpusName, String fileName) throws Exception {
    	CorpusBatch corpusBatch = new CorpusBatch();
    	ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    	corpusBatch.setData(
    			mapper.readValue(
    					new File(fileName), 
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
		getLogger().debug(sortedSteps.size());
		Collections.sort(sortedSteps);
		ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		ICorpusBatchStep previousBatchStep = null;
		ICorpusBatchStep currentBatchStep = null;
    	for (CorpusBatchStepModel corpusBatchStepModel : sortedSteps) {
    		previousBatchStep = currentBatchStep;
    		currentBatchStep = (ICorpusBatchStep) Class.forName(corpusBatchStepModel.getCorpusBatchStepClass()).newInstance();
    		currentBatchStep.setData(corpusBatchStepModel);
    		currentBatchStep.setStorage(getScratchStorage());
    		currentBatchStep.setParameterStore(getParameterStore());
    		currentBatchStep.getData().setCorpusId(getData().getCorpusId());
    		currentBatchStep.getData().setCorpusBatchId(getData().getCorpusBatchId());
    		getLogger().debug(getData().getCorpusId() + " " + getData().getCorpusBatchId() + " " + currentBatchStep.getData().getCorpusBatchStepId());
    		if (previousBatchStep != null) {
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
		ICorpusBatchStep previousBatchStep = null;
		ICorpusBatchStep currentBatchStep = null;
    	for (CorpusBatchStepModel corpusBatchStepModel : sortedSteps) {
    		previousBatchStep = currentBatchStep;
    		currentBatchStep = (ICorpusBatchStep) Class.forName(corpusBatchStepModel.getCorpusBatchStepClass()).newInstance();
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
