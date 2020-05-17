package io.outofprintmagazine.corpus.batch.impl;

import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class CoreNLPTfidfScores extends CorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(CoreNLPTfidfScores.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	public CoreNLPTfidfScores() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode input) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(input);
		ObjectNode documentAggregates = (ObjectNode) getJsonNodeFromStorage(input, "oopNLPAggregatesStorage");
		ObjectNode corpusIdfScores = (ObjectNode) getJsonNodeFromStorage(input, "oopNLPCorpusIdfScoresStorage");
		ObjectNode documentTfidfScores = getMapper().createObjectNode();

		calculateTfidfScores(corpusIdfScores, documentAggregates, documentTfidfScores);

		outputStepItem.put(
				"oopNLPTfidfScoresStorage",
				getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("TfidfScores_" + getDocID(input), "json"),
					documentTfidfScores
			)
		);
		
		retval.add(outputStepItem);
		return retval;
	}
	

	protected void calculateTfidfScores(ObjectNode corpusIdfScores, ObjectNode documentAggregates, ObjectNode documentTfidfScores) {

		Iterator<String> annotationNameIter = documentAggregates.fieldNames();
		while (annotationNameIter.hasNext()) {
			String annotationName = annotationNameIter.next();
			if (documentAggregates.get(annotationName).isObject()) {
				if (annotationName.equals("metadata")) {
					documentTfidfScores.set(annotationName, documentAggregates.get(annotationName).deepCopy());
				}
				else if (corpusIdfScores.has(annotationName) && corpusIdfScores.get(annotationName).isObject()) {
					ObjectNode documentAnnotationScoreStats = (ObjectNode) documentAggregates.get(annotationName);
					ObjectNode documentAnnotationTfidf = documentTfidfScores.putObject(annotationName);
					documentAnnotationTfidf.put("name", annotationName);
					ArrayNode aggregatedScores = (ArrayNode) documentAnnotationScoreStats.get("aggregatedScores");
					ArrayNode aggregatedTfidfScores = documentAnnotationTfidf.putArray("aggregatedScores");
					Iterator<JsonNode> aggregatedScoreIter = aggregatedScores.iterator();
					while (aggregatedScoreIter.hasNext()) {
						ObjectNode aggregatedScore = (ObjectNode) aggregatedScoreIter.next();
						String subscoreName = aggregatedScore.get("name").asText();
						if (corpusIdfScores.get(annotationName).has(subscoreName)) {
							double tf = aggregatedScore.get("score").get("raw").asDouble();
							double n = corpusIdfScores.get(annotationName).get(subscoreName).get("corpusSize").asDouble();
							double d = corpusIdfScores.get(annotationName).get(subscoreName).get("documentCount").asDouble();
							BigDecimal tfidfValue = new BigDecimal(tf * (1 + (java.lang.Math.log((n/(1+d))))));
							ObjectNode tfidfNode = getMapper().createObjectNode();
							tfidfNode.put("name", subscoreName);
							tfidfNode.put("tfidf", tfidfValue);
							aggregatedTfidfScores.add(tfidfNode);
						}
					}
				}
			}
		}
	}
	
	
	@Override
	public void configure(ObjectNode properties) {
		getData().setProperties(properties);
	}

}
