package io.outofprintmagazine.corpus.batch.impl.word2vec;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import io.outofprintmagazine.corpus.batch.CorpusBatchStepTask;

public class Word2VecProcessor extends CorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(Word2VecProcessor.class);
		
	public Word2VecProcessor() {
		super();
	}

	protected List<String> corpusBatchStepTasks = null;
	
	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	@Override
	public ArrayNode run(ArrayNode input) {
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
		return super.run(input);
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

	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		if (corpusBatchStepTasks != null) {
			for (String task : corpusBatchStepTasks) {
				Word2Vec vec = processWord2Vec(
						getTextDocumentFromStorage(
								outputStepItem, 
								"word2vec_" + task + "_Preprocessed"
						)
				);
				
				File f = null;
				FileInputStream fin = null;
				try {
					f = File.createTempFile(getDocID(outputStepItem), "vec");
			        WordVectorSerializer.writeWord2VecModel(vec, f);
					fin = new FileInputStream(f);			        
					outputStepItem.put(
							"word2vec_" + task + "_Processed",
							getStorage().storeScratchFileStream(
								getData().getCorpusId(),
								getOutputScratchFilePath(task + "_" + getDocID(outputStepItem), "vec"),
								fin
						)
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
				
			}
		}
        
		retval.add(outputStepItem);
		return retval;
	}
}
