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
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class CoreNLPZScores extends CorpusBatchStep implements ICorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(CoreNLPZScores.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
	
	public CoreNLPZScores() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode input) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(input);
		ObjectNode documentAggregates = (ObjectNode) getJsonNodeFromStorage(input, "oopNLPAggregatesStorage");
		ObjectNode corpusAggregateScores = (ObjectNode) getJsonNodeFromStorage(input, "oopNLPCorpusAggregatesStorage");
		ObjectNode documentZScores = getMapper().createObjectNode();

		calculateZScores(corpusAggregateScores, documentAggregates, documentZScores);
		calculateZSubScores(corpusAggregateScores, documentAggregates, documentZScores);
		
		outputStepItem.put(
				"oopNLPZScoresStorage",
				getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("ZScores_" + getDocID(input), "json"),
					documentZScores
			)
		);
		
		retval.add(outputStepItem);
		return retval;
	}
	
	protected void calculateZSubScores(ObjectNode corpusAggregates, ObjectNode documentAggregates, ObjectNode documentZScores) {
	    List<String> scoreMeasures = Arrays.asList("raw", "normalized", "count");
	    List<String> statsMeasures = Arrays.asList("rank", "percentage", "percentile");
		Iterator<String> annotationNameIter = documentAggregates.fieldNames();
		while (annotationNameIter.hasNext()) {
			String annotationName = annotationNameIter.next();
			if (!annotationName.equals("metadata") && documentAggregates.get(annotationName).isObject() && corpusAggregates.has(annotationName) && corpusAggregates.get(annotationName).isObject()) {
				ObjectNode documentAnnotationScoreStats = (ObjectNode) documentAggregates.get(annotationName);
				ArrayNode aggregatedScores = (ArrayNode) documentAnnotationScoreStats.get("aggregatedScores");
				ArrayNode aggregatedZScores = ((ObjectNode) documentZScores.get(annotationName)).putArray("aggregatedScores");
				Iterator<JsonNode> aggregatedScoreIter = aggregatedScores.iterator();
				while (aggregatedScoreIter.hasNext()) {
					ObjectNode documentAnnotationSubScoreStatsScore = (ObjectNode) aggregatedScoreIter.next();
					String subscoreName = documentAnnotationSubScoreStatsScore.get("name").asText();
					ObjectNode corpusAnnotationScoreStats = getSubscoreFromCorpusAggregate(corpusAggregates, annotationName, subscoreName);
					if (corpusAnnotationScoreStats != null) {
						ObjectNode aggregatedZScore = getMapper().createObjectNode();
						aggregatedZScore.put("name", subscoreName);
						aggregatedZScore.putObject("score");
						aggregatedZScore.putObject("aggregateScore");
						aggregatedZScores.add(aggregatedZScore);
						if (documentAnnotationSubScoreStatsScore != null) {
							for (String scoreMeasure : scoreMeasures) {
				    			BigDecimal corpusStddev = corpusAnnotationScoreStats.get("score").get(scoreMeasure).get("stddev").decimalValue();
				    			BigDecimal zScore = new BigDecimal(0);
				    			if (! corpusStddev.equals(new BigDecimal(0))) {		
					    			BigDecimal corpusMean = corpusAnnotationScoreStats.get("score").get(scoreMeasure).get("mean").decimalValue();
					    			BigDecimal documentScore = documentAnnotationSubScoreStatsScore.get("score").get(scoreMeasure).decimalValue();
					    			zScore = (documentScore.subtract(corpusMean)).divide(corpusStddev, 10, RoundingMode.HALF_DOWN);
				    			} 
				    			((ObjectNode)aggregatedZScore.get("score")).put(scoreMeasure, zScore);
				    		}
							for (String statsMeasure : statsMeasures) {
				    			BigDecimal corpusStddev = corpusAnnotationScoreStats.get("aggregateScore").get(statsMeasure).get("stddev").decimalValue();
				    			BigDecimal zScore = new BigDecimal(0);
				    			if (! corpusStddev.equals(new BigDecimal(0))) {		    				
					    			BigDecimal corpusMean = corpusAnnotationScoreStats.get("aggregateScore").get(statsMeasure).get("mean").decimalValue();
					    			BigDecimal documentScore = documentAnnotationSubScoreStatsScore.get("aggregateScore").get(statsMeasure).decimalValue();
					    			zScore = (documentScore.subtract(corpusMean)).divide(corpusStddev, 10, RoundingMode.HALF_DOWN);
				    			}
				    			((ObjectNode)aggregatedZScore.get("aggregateScore")).put(statsMeasure, zScore);				
							}
						}
					}
				}
			}
		}
	}

	protected void calculateZScores(ObjectNode corpusAggregates, ObjectNode documentAggregates, ObjectNode documentZScores) {
	    List<String> scoreMeasures = Arrays.asList("raw", "normalized", "count");
	    List<String> statsMeasures = Arrays.asList("min", "mean", "median", "max");
		Iterator<String> annotationNameIter = documentAggregates.fieldNames();
		while (annotationNameIter.hasNext()) {
			String annotationName = annotationNameIter.next();
			if (documentAggregates.get(annotationName).isObject()) {
				if (annotationName.equals("metadata")) {
					documentZScores.set(annotationName, documentAggregates.get(annotationName).deepCopy());
				}
				else if (corpusAggregates.has(annotationName) && corpusAggregates.get(annotationName).isObject()) {
					ObjectNode documentAnnotationScoreStats = (ObjectNode) documentAggregates.get(annotationName);
					ObjectNode corpusAnnotationScoreStats = (ObjectNode) corpusAggregates.get(annotationName);
					ObjectNode documentAnnotationZScoreStats = documentZScores.putObject(annotationName).putObject("scoreStats");
					ObjectNode documentAnnotationZScoreStatsScore = documentAnnotationZScoreStats.putObject("score");
					ObjectNode documentAnnotationZScoreStatsStats = documentAnnotationZScoreStats.putObject("stats");
			    	for (String scoreMeasure : scoreMeasures) {
			    		ObjectNode documentAnnotationScoreStatsScore = (ObjectNode) documentAnnotationScoreStats.get("scoreStats").get("score");
			    		ObjectNode corpusAnnotationScoreStatsScore = (ObjectNode) corpusAnnotationScoreStats.get("score").get(scoreMeasure);		    		
	
		    			BigDecimal corpusStddev = corpusAnnotationScoreStatsScore.get("stddev").decimalValue();
		    			BigDecimal zScore = new BigDecimal(0);
		    			if (! corpusStddev.equals(new BigDecimal(0))) {		    				
			    			BigDecimal corpusMean = corpusAnnotationScoreStatsScore.get("mean").decimalValue();
			    			BigDecimal documentScore = documentAnnotationScoreStatsScore.get(scoreMeasure).decimalValue();
			    			zScore = (documentScore.subtract(corpusMean)).divide(corpusStddev, 10, RoundingMode.HALF_DOWN);
		    			}
		    			documentAnnotationZScoreStatsScore.put(scoreMeasure, zScore);
	
			    	}
			    	for (String statsMeasure : statsMeasures) {
			    		ObjectNode documentAnnotationScoreStatsScore = (ObjectNode) documentAnnotationScoreStats.get("scoreStats").get("stats");
			    		ObjectNode corpusAnnotationScoreStatsScore = (ObjectNode) corpusAnnotationScoreStats.get("stats").get(statsMeasure);		    		
		    			BigDecimal corpusStddev = corpusAnnotationScoreStatsScore.get("stddev").decimalValue();
		    			BigDecimal zScore = new BigDecimal(0);
		    			if (! corpusStddev.equals(new BigDecimal(0))) {		    				
			    			BigDecimal corpusMean = corpusAnnotationScoreStatsScore.get("mean").decimalValue();
			    			BigDecimal documentScore = documentAnnotationScoreStatsScore.get(statsMeasure).decimalValue();
			    			zScore = (documentScore.subtract(corpusMean)).divide(corpusStddev, 10, RoundingMode.HALF_DOWN);
		    			}
		    			documentAnnotationZScoreStatsStats.put(statsMeasure, zScore);
			    	}
				}
			}
		}
	}
	
	protected ObjectNode getSubscoreFromCorpusAggregate(ObjectNode corpusAggregates, String score, String subscore) {
		ObjectNode corpusAnnotationScoreStats = (ObjectNode) corpusAggregates.get(score);
		if (corpusAnnotationScoreStats.has("aggregatedScores")) {
			ArrayNode aggregatedScores = (ArrayNode) corpusAnnotationScoreStats.get("aggregatedScores");
			for (JsonNode aggregatedScore : aggregatedScores) {
				if (aggregatedScore.get("name").asText().equals(subscore)) {
					return (ObjectNode) aggregatedScore;
				}
			}
		}
		getLogger().debug("score: " + score,  "subscore: " + subscore + " was null");
		return null;
	}
}
