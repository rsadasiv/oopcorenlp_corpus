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
package io.outofprintmagazine.corpus.batch.impl;

import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class CoreNLPTfidfScores extends CorpusBatchStep implements ICorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(CoreNLPTfidfScores.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
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
							double tf = aggregatedScore.get("score").get("normalized").asDouble();
							double n = corpusIdfScores.get(annotationName).get(subscoreName).get("corpusSize").asDouble();
							double d = corpusIdfScores.get(annotationName).get(subscoreName).get("documentCount").asDouble();
							BigDecimal tfidfValue = new BigDecimal(0);
							if (d != 0) {
								tfidfValue = new BigDecimal(tf * (java.lang.Math.log((n/d))));
							}
							ObjectNode tfidfNode = getMapper().createObjectNode();
							tfidfNode.put("name", subscoreName);
							tfidfNode.put("value", tfidfValue);
							aggregatedTfidfScores.add(tfidfNode);
						}
					}
				}
			}
		}
	}
}
