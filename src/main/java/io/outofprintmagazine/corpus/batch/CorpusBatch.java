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
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
	
	public IScratchStorage getScratchStorage() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		if (scratchStorage == null) {
			scratchStorage = (IScratchStorage) Class.forName(getData().getScratchStorageClass()).getConstructor().newInstance();
			scratchStorage.setParameterStore(getParameterStore());
		}
		return scratchStorage;
	}
	
	public void setScratchStorage(IScratchStorage storage) {
		this.scratchStorage = storage;
	}
	
	private IBatchStorage batchStorage = null;
	
	public IBatchStorage getBatchStorage() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		if (batchStorage == null) {
			batchStorage = (IBatchStorage) Class.forName(getData().getBatchStorageClass()).getConstructor().newInstance();
			batchStorage.setParameterStore(getParameterStore());
		}
		return batchStorage;
	}
	
	public void setBatchStorage(IBatchStorage batchStorage) {
		this.batchStorage = batchStorage;
	}
	
	private IParameterStore parameterStore = null;
	
	public IParameterStore getParameterStore() throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		if (parameterStore == null) {
			parameterStore = (IParameterStore) Class.forName(getData().getParameterStoreClass()).getConstructor().newInstance();
			parameterStore.init(getData().getProperties());
		}
		return parameterStore;
	}
	
	public void setParameterStore(IParameterStore parameterStore) {
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
	
	private List<String> readStreamToList(InputStream in) throws IOException {
		try {
			List<String> allLines = IOUtils.readLines(
					in,
					StandardCharsets.UTF_8.name()
			);
			List<String> retval = new ArrayList<String>();
			for (String line : allLines) {
				if (!line.startsWith("#") && !line.startsWith("/")) {
					retval.add(line);
				}
			}
			return retval;
		}
		finally {
			in.close();
		}
	}
	
	public void appendAnalyzeStep() throws IOException {
		CorpusBatchStepModel analyzeStep = new CorpusBatchStepModel();
		analyzeStep.setCorpusBatchId(getData().getCorpusBatchId());
		analyzeStep.setCorpusBatchStepSequenceId(Integer.valueOf(getData().getCorpusBatchSteps().size()));
		analyzeStep.setCorpusBatchStepId("Analyze");
		analyzeStep.setCorpusBatchStepClass("io.outofprintmagazine.corpus.batch.impl.Analyze");
		ArrayNode customAnnotators = analyzeStep.getProperties().arrayNode();
		List<String> customAnnotatorList = readStreamToList(this.getClass().getClassLoader().getResourceAsStream("io/outofprintmagazine/util/annotators.txt"));
		for (String customAnnotator : customAnnotatorList) {
			customAnnotators.add(customAnnotator);
		}
		analyzeStep.getProperties().set("customAnnotators", customAnnotators);
		getData().getCorpusBatchSteps().add(analyzeStep);
	}
	
	public void appendAggregateStep()  {
		CorpusBatchStepModel aggregateStep = new CorpusBatchStepModel();
		aggregateStep.setCorpusBatchId(getData().getCorpusBatchId());
		aggregateStep.setCorpusBatchStepSequenceId(Integer.valueOf(getData().getCorpusBatchSteps().size()));
		aggregateStep.setCorpusBatchStepId("CorpusAggregate");
		aggregateStep.setCorpusBatchStepClass("io.outofprintmagazine.corpus.batch.impl.CorpusAggregate");
		getData().getCorpusBatchSteps().add(aggregateStep);

		CorpusBatchStepModel aggregateIdfStep = new CorpusBatchStepModel();
		aggregateIdfStep.setCorpusBatchId(getData().getCorpusBatchId());
		aggregateIdfStep.setCorpusBatchStepSequenceId(Integer.valueOf(getData().getCorpusBatchSteps().size()));
		aggregateIdfStep.setCorpusBatchStepId("CoreNLPTfidf");
		aggregateIdfStep.setCorpusBatchStepClass("io.outofprintmagazine.corpus.batch.impl.CoreNLPTfidfScores");
		getData().getCorpusBatchSteps().add(aggregateIdfStep);		

		CorpusBatchStepModel aggregateZStep = new CorpusBatchStepModel();
		aggregateZStep.setCorpusBatchId(getData().getCorpusBatchId());
		aggregateZStep.setCorpusBatchStepSequenceId(Integer.valueOf(getData().getCorpusBatchSteps().size()));
		aggregateZStep.setCorpusBatchStepId("CoreNLPZ");
		aggregateZStep.setCorpusBatchStepClass("io.outofprintmagazine.corpus.batch.impl.CoreNLPZScores");
		getData().getCorpusBatchSteps().add(aggregateZStep);	

		CorpusBatchStepModel aggregateMBStep = new CorpusBatchStepModel();
		aggregateMBStep.setCorpusBatchId(getData().getCorpusBatchId());
		aggregateMBStep.setCorpusBatchStepSequenceId(Integer.valueOf(getData().getCorpusBatchSteps().size()));
		aggregateMBStep.setCorpusBatchStepId("CoreNLPMB");
		aggregateMBStep.setCorpusBatchStepClass("io.outofprintmagazine.corpus.batch.impl.CoreNLPMyersBriggsScores");
		getData().getCorpusBatchSteps().add(aggregateMBStep);		
		
		CorpusBatchStepModel word2vecStep = new CorpusBatchStepModel();
		word2vecStep.setCorpusBatchId(getData().getCorpusBatchId());
		word2vecStep.setCorpusBatchStepSequenceId(Integer.valueOf(getData().getCorpusBatchSteps().size()));
		word2vecStep.setCorpusBatchStepId("DocumentWord2Vec");
		word2vecStep.setCorpusBatchStepClass("io.outofprintmagazine.corpus.batch.impl.DocumentWord2Vec");
		getData().getCorpusBatchSteps().add(word2vecStep);
		
		CorpusBatchStepModel corpusWord2vecStep = new CorpusBatchStepModel();
		corpusWord2vecStep.setCorpusBatchId(getData().getCorpusBatchId());
		corpusWord2vecStep.setCorpusBatchStepSequenceId(Integer.valueOf(getData().getCorpusBatchSteps().size()));
		corpusWord2vecStep.setCorpusBatchStepId("CorpusWord2Vec");
		corpusWord2vecStep.setCorpusBatchStepClass("io.outofprintmagazine.corpus.batch.impl.CorpusWord2Vec");
		getData().getCorpusBatchSteps().add(corpusWord2vecStep);
	}
	
	public void aggregateBatches(List<CorpusBatch> batches) {
		CorpusBatchStepModel aggregateStep = new CorpusBatchStepModel();
		aggregateStep.setCorpusBatchId(getData().getCorpusBatchId());
		aggregateStep.setCorpusBatchStepSequenceId(Integer.valueOf(getData().getCorpusBatchSteps().size()));
		aggregateStep.setCorpusBatchStepId("CorpusAggregate");
		aggregateStep.setCorpusBatchStepClass("io.outofprintmagazine.corpus.batch.impl.CorporaAggregate");
		aggregateStep.getProperties().put("noCache", "true");
		getData().getCorpusBatchSteps().add(aggregateStep);
		for (CorpusBatch batch : batches) {
			for (JsonNode batchOutput : batch.getData().getCorpusBatchSteps().get(batch.getData().getCorpusBatchSteps().size()-1).getOutput()) {
				ObjectNode sourceOutputItem = (ObjectNode) batchOutput.deepCopy();
				sourceOutputItem.put("sourceCorpusId", batch.getData().getCorpusId());
				getData().getCorpusBatchSteps().get(0).getInput().add(sourceOutputItem);
			}
		}
	}
	
	public static CorpusBatch buildFromStagingBatch(String corpusName, String batchName) throws Exception {
		return buildFromStagingBatch(corpusName, batchName, "io.outofprintmagazine.corpus.storage.file.FileCorpora");
	}
	
	public static CorpusBatch buildFromStagingBatch(String corpusName, String batchName, String batchStorageClass) throws Exception {
    	IBatchStorage batchStorage = (IBatchStorage) Class.forName(batchStorageClass).getConstructor().newInstance();
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
	
	public static CorpusBatch buildFromFile(String fileName) throws Exception {
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

    		currentBatchStep = (ICorpusBatchStep) Class.forName(corpusBatchStepModel.getCorpusBatchStepClass()).getConstructor().newInstance();
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
    		getBatchStorage().storeStagingBatchString(getData().getCorpusId(), getData().getCorpusBatchId(), mapper.writeValueAsString(getData()));
    		previousBatchStep = currentBatchStep;
    	}
    	//save incrementally
    	//getBatchStorage().storeStagingBatchString(getData().getCorpusId(), getData().getCorpusBatchId(), mapper.writeValueAsString(getData()));
	}
	
	public void runStep(String stepId) throws Exception {
		List<CorpusBatchStepModel> sortedSteps = new ArrayList<CorpusBatchStepModel>(getData().getCorpusBatchSteps());
		Collections.sort(sortedSteps);
		ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		ICorpusBatchStep previousBatchStep = null;
		ICorpusBatchStep currentBatchStep = null;
    	for (CorpusBatchStepModel corpusBatchStepModel : sortedSteps) {
    		previousBatchStep = currentBatchStep;
    		currentBatchStep = (ICorpusBatchStep) Class.forName(corpusBatchStepModel.getCorpusBatchStepClass()).getConstructor().newInstance();
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
