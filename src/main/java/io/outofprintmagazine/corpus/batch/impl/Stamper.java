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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class Stamper extends CorpusBatchStep implements ICorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(Stamper.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}

	public Stamper() {
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
		if (getData().getProperties().hasNonNull("stamps")) {
			ArrayNode stamps = (ArrayNode)getData().getProperties().get("stamps");
			Iterator<JsonNode> stampsIter = stamps.elements();
			while (stampsIter.hasNext()) {
				ObjectNode stamp = (ObjectNode) stampsIter.next();
				ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
				Iterator<Entry<String, JsonNode>> fieldsIter = stamp.fields();
				while (fieldsIter.hasNext()) {
					Entry<String,JsonNode> field = fieldsIter.next();
					outputStepItem.set(field.getKey(), field.getValue());
				}
				retval.add(outputStepItem);
			}
		}
		return retval;
	}
	
}
