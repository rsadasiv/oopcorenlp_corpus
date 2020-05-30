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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStepTask;
import io.outofprintmagazine.corpus.batch.ThreadedCorpusBatchStepTask;
import io.outofprintmagazine.corpus.storage.DocumentStorage;
import io.outofprintmagazine.corpus.storage.postgresql.PostgreSQLDocumentStorage;

public class PostgreSQLCoreNLPZScoresLoader extends ThreadedCorpusBatchStepTask implements CorpusBatchStepTask {

	private static final Logger logger = LogManager.getLogger(PostgreSQLCoreNLPZScoresLoader.class);
	
	private DocumentStorage loader = null;
	
	public PostgreSQLCoreNLPZScoresLoader() {
		super();
	}

	@Override
	protected Logger getLogger() {
		return logger;
	}

	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		if (loader == null) {
			loader = new PostgreSQLDocumentStorage(getParameterStore());
		}
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);

		loader.storeOOPZScores(
				getData().getCorpusId(),
				getData().getCorpusBatchId(),
				getDocID(inputStepItem),
				(ObjectNode) getJsonNodeFromStorage(inputStepItem, "oopNLPZScoresStorage")
		);
		

		outputStepItem.put("PostgreSQLCoreNLPZScoresLoader", "true");
		retval.add(outputStepItem);
		return retval;
	}
	
}
