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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class CorporaAggregate extends CorpusAggregate implements ICorpusBatchStep {

	public CorporaAggregate() {
		super();
	}

	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		JsonNode aggregateNode = getMapper().readTree(
				getStorage().getScratchFileStream(
						inputStepItem.get("sourceCorpusId").asText(),
						inputStepItem.get("oopNLPAggregatesStorage").asText()
				)
		);
		if (aggregateNode != null && !aggregateNode.isNull()) {
			storeOOPAggregates(
				getData().getCorpusId(), 
				getDocID(inputStepItem),
				(ObjectNode) aggregateNode
			);
		}
				
		retval.add(outputStepItem);
		return retval;
	}
	
}
