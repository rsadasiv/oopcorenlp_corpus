package io.outofprintmagazine.corpus.batch.impl;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import edu.stanford.nlp.ling.CoreAnnotations;
import io.outofprintmagazine.Analyzer;
import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class CoreNLP extends CorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(CoreNLP.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	private Analyzer ta = null;
	
	public CoreNLP() {
		ta = new Analyzer();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		try {
			String doc = getTextDocumentFromStorage(inputStepItem);
			
			ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
//			ObjectNode storageProperties = getMapper().createObjectNode();
//			//"Sun, 16 Feb 2020 23:17:38 GMT"
//			storageProperties.put("Content-Type", "text/plain");
//			storageProperties.put("mimeType", "text/plain");
//			storageProperties.put("charset",  "us-ascii");
//			storageProperties.put("Date", getDateFormat().format(new Date(System.currentTimeMillis())));
	
			Properties metadata = new Properties();
			metadata.put(CoreAnnotations.DocIDAnnotation.class.getName(), getDocID(inputStepItem));
			metadata.put(CoreAnnotations.DocTypeAnnotation.class.getName(), getData().getCorpusId());
			metadata.put(CoreAnnotations.AuthorAnnotation.class.getName(), getAuthor(inputStepItem));
			metadata.put(CoreAnnotations.DocDateAnnotation.class.getName(), getDate(inputStepItem));
			metadata.put(CoreAnnotations.DocTitleAnnotation.class.getName(), getTitle(inputStepItem));
			metadata.put(CoreAnnotations.DocSourceTypeAnnotation.class.getName(), getLink(inputStepItem));
			
			Map<String,ObjectNode> json = ta.analyze(metadata, doc);
						
			outputStepItem.put(
				"coreNLPStorage",
				getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("STANFORD_" + getDocID(inputStepItem), "json"),
					json.get("STANFORD")
				)
			);
			
			outputStepItem.put(
				"oopNLPStorage",
				getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("OOP_" + getDocID(inputStepItem), "json"),
					json.get("OOP")
				)
			);
			
			outputStepItem.put(
					"oopNLPAggregatesStorage",
					getStorage().storeScratchFileObject(
						getData().getCorpusId(),
						getOutputScratchFilePath("AGGREGATES_" + getDocID(inputStepItem), "json"),
						json.get("AGGREGATES")
				)
			);
			
			outputStepItem.put(
				"pipelineStorage",
				getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("PIPELINE_" + getDocID(inputStepItem), "json"),
					json.get("PIPELINE")
				)
			);
	
			retval.add(outputStepItem);
		}
		catch (ClassNotFoundException e) {
			getLogger().error(e);
			throw new IOException(e);
		} 
		catch (InstantiationException e) {
			getLogger().error(e);
			throw new IOException(e);
		} 
		catch (IllegalAccessException e) {
			getLogger().error(e);
			throw new IOException(e);
		}
		return retval;
	}
	
	@Override
	public void configure(ObjectNode properties) {
		getData().setProperties(properties);
	}

}
