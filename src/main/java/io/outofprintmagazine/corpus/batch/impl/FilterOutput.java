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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class FilterOutput extends CorpusBatchStep implements ICorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(FilterOutput.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}

	public FilterOutput() {
		super();
	}
	
	@Override
	public ArrayNode run(ArrayNode input) {
		for (JsonNode inputItem : input) {
			try {
				ArrayNode generatedOutput = runOne((ObjectNode)inputItem);
				for (JsonNode generatedOutputItem : generatedOutput) {
					getData().getOutput().add(generatedOutputItem);
				}
			}
			catch (Throwable t) {
				t.printStackTrace();
				getLogger().error(t);
			}
		}
		return getData().getOutput();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		if (filterMatch(inputStepItem)) {
			retval.add(copyInputToOutput(inputStepItem));
		}
		return retval;
	}
	
	private boolean filterMatch(ObjectNode inputStepItem) {
		if (getData().getProperties().hasNonNull("filters")) {
			ArrayNode filters = (ArrayNode)getData().getProperties().get("filters");
			Iterator<Entry<String, JsonNode>> fieldsIter = inputStepItem.fields();
			while (fieldsIter.hasNext()) {
				Entry<String,JsonNode> field = fieldsIter.next();
				Iterator<JsonNode> filterIter = filters.elements();
				while (filterIter.hasNext()) {
					ObjectNode filter = (ObjectNode) filterIter.next();
					if (filter.has(field.getKey())) {
						if (filter.get(field.getKey()).asText().equals(field.getValue().asText())) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

}
