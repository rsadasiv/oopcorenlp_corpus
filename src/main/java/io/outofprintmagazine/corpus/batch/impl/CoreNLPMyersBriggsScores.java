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
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;

public class CoreNLPMyersBriggsScores extends CorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(CoreNLPMyersBriggsScores.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	public CoreNLPMyersBriggsScores() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode input) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(input);
		ObjectNode documentScores = (ObjectNode) getJsonNodeFromStorage(input, "oopNLPStorage");
		ObjectNode corpusMyersBriggsScores = (ObjectNode) getJsonNodeFromStorage(input, "oopNLPCorpusMyersBriggsAggregateScoresStorage");
		ObjectNode documentMyersBriggsScores = getMapper().createObjectNode();

		calculateMyersBriggsScores(corpusMyersBriggsScores, documentScores, documentMyersBriggsScores);

		outputStepItem.put(
				"oopNLPMyersBriggsScoresStorage",
				getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("MyersBriggsScores_" + getDocID(input), "json"),
					documentMyersBriggsScores
			)
		);
		
		retval.add(outputStepItem);
		return retval;
	}
	

	protected void calculateMyersBriggsScores(ObjectNode corpusMyersBriggsScores, ObjectNode documentScores, ObjectNode documentMyersBriggsScores) {
		ObjectNode documentMyersBriggsScoresDocumentNode = documentMyersBriggsScores.putObject("OOPMyersBriggsAnnotation");
		ObjectNode documentMyersBriggsScoresActorsNode = documentMyersBriggsScores.putObject("OOPActorsAnnotation");
		BigDecimal tokenCount = new BigDecimal(documentScores.get("OOPWordCountAnnotation").asInt());
		Iterator<String> corpusSubscoreNameIter = corpusMyersBriggsScores.fieldNames();
		while (corpusSubscoreNameIter.hasNext()) {
			String subscoreName = corpusSubscoreNameIter.next();
			BigDecimal documentSubscoreScore = new BigDecimal(0);
			BigDecimal corpusSubscoreScore = new BigDecimal(corpusMyersBriggsScores.get(subscoreName).asText());
			//TODO - this is always zero
			if (documentScores.get("OOPMyersBriggsAnnotation").has(subscoreName)) {
				documentSubscoreScore = new BigDecimal(documentScores.get("OOPMyersBriggsAnnotation").get(subscoreName).asText()).divide(tokenCount, 10, RoundingMode.HALF_DOWN);
			}
			documentMyersBriggsScoresDocumentNode.put(subscoreName, documentSubscoreScore.divide(corpusSubscoreScore, 10, RoundingMode.HALF_DOWN));
			Iterator<String> actorNameIter = documentScores.get("OOPActorsAnnotation").fieldNames();
			while (actorNameIter.hasNext()) {
				String actorName = actorNameIter.next();
				if (! documentMyersBriggsScoresActorsNode.has(actorName)) {
					documentMyersBriggsScoresActorsNode.putObject(actorName).putObject("oopmyersBriggs");
				}
				BigDecimal actorSubscoreScore = new BigDecimal(0);
				if (documentScores.get("OOPActorsAnnotation").get(actorName).get("oopmyersBriggs").has(subscoreName)) {
					actorSubscoreScore = new BigDecimal(documentScores.get("OOPActorsAnnotation").get(actorName).get("oopmyersBriggs").get(subscoreName).asText()).divide(tokenCount, 10, RoundingMode.HALF_DOWN);
				}
				((ObjectNode) documentMyersBriggsScoresActorsNode.get(actorName).get("oopmyersBriggs")).put(subscoreName, actorSubscoreScore.divide(corpusSubscoreScore, 10, RoundingMode.HALF_DOWN));
			}
		}
	}
	
	
	@Override
	public void configure(ObjectNode properties) {
		getData().setProperties(properties);
	}

}
