package io.outofprintmagazine.corpus.batch.impl.word2vec;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.BasicLineIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class Word2VecCorpusProcessor extends CorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(Word2VecCorpusProcessor.class);
		
	public Word2VecCorpusProcessor() {
		super();
	}

	protected List<String> corpusBatchStepTasks = null;
	protected Map<String, StringBuffer> corpusBatchStepTaskInput = new HashMap<String, StringBuffer>();
	protected Map<String, String> corpusBatchStepTaskOutput = new HashMap<String, String>();
	
	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	
	@Override
	public ArrayNode run(ArrayNode input) {
		//initialize cache
		corpusBatchStepTaskInput = new HashMap<String, StringBuffer>();
		corpusBatchStepTaskOutput = new HashMap<String, String>();
		
		//initialize tasks
		if (corpusBatchStepTasks == null) {
			corpusBatchStepTasks = new ArrayList<String>();
			if (getData().getProperties().has("tasks")) {
				ArrayNode tasks = (ArrayNode) getData().getProperties().get("tasks");
				Iterator<JsonNode> tasksIter = tasks.elements();
				while (tasksIter.hasNext()) {
					corpusBatchStepTasks.add(tasksIter.next().asText());
				}
			}
		}

		//iterate through all input items and concatenate texts per task
		try {
			for (JsonNode inputItem : input) {
				addToCache((ObjectNode) inputItem);
			}
		}
		catch (Exception e) {
			getLogger().error(e);
			return getData().getOutput();
		}

		//iterate through each task and create corpus vec per task
		try {
			for (String task : corpusBatchStepTasks) {
				corpusBatchStepTaskOutput.put(task,
						storeWord2Vec(
								processWord2Vec(
										corpusBatchStepTaskInput.get(task).toString()
								),
								task
						)
				);
			}
		}
		catch (Exception e) {
			getLogger().error(e);
			return getData().getOutput();
		}
		
		//return super
		return super.run(input);
	}
	
	@Override
	public ArrayNode runOne(ObjectNode input) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(input);
		for (String task : corpusBatchStepTasks) {
			outputStepItem.put(
				"corpus_word2vec_" + task + "_Processed",
				corpusBatchStepTaskOutput.get(task)
			);
		}
		retval.add(outputStepItem);
		return retval;
	}
	
	protected String storeWord2Vec(Word2Vec vec, String taskName) throws Exception {
		File f = null;
		FileInputStream fin = null;
		String retval = "";
		try {
			f = File.createTempFile(taskName, "vec");
	        WordVectorSerializer.writeWord2VecModel(vec, f);
			fin = new FileInputStream(f);			        
			retval = getStorage().storeScratchFileStream(
						getData().getCorpusId(),
						getOutputScratchFilePath("corpus_" + taskName, "vec"),
						fin
			);
	        
			fin.close();
		}
		catch (Exception e) {
			throw e;
		}
		finally {
			if (fin != null) {
				fin.close();
				fin = null;
			}
			if (f != null) {
				f.delete();
				f = null;
			}
		}
		return retval;
	}
	
	
	protected Word2Vec processWord2Vec(String doc) throws IOException {
		SentenceIterator iter = new BasicLineIterator(IOUtils.toInputStream(doc, "utf-8"));
		TokenizerFactory t = new DefaultTokenizerFactory();
		t.setTokenPreProcessor(new CommonPreprocessor());

        Word2Vec vec = new Word2Vec.Builder()
                .minWordFrequency(5)
                .iterations(1)
                .layerSize(100)
                .seed(42)
                .windowSize(5)
                .iterate(iter)
                .tokenizerFactory(t)
                .build();

        vec.fit();
        return vec;
				
	}


	public void addToCache(ObjectNode inputStepItem) throws Exception {
		if (corpusBatchStepTasks != null) {
			for (String task : corpusBatchStepTasks) {
				String doc = getTextDocumentFromStorage(inputStepItem, "word2vec_" + task + "_Preprocessed");
				StringBuffer corpusBatchStepTaskInputBuffer = corpusBatchStepTaskInput.get(task);
				if (corpusBatchStepTaskInputBuffer == null) {
					corpusBatchStepTaskInputBuffer = new StringBuffer();
					corpusBatchStepTaskInput.put(task, corpusBatchStepTaskInputBuffer);
				}
				corpusBatchStepTaskInputBuffer.append(doc);
				corpusBatchStepTaskInputBuffer.append("\\n\\n\\n\\n");
			}
		}

	}
}
