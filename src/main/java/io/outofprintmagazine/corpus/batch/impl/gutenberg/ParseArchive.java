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
package io.outofprintmagazine.corpus.batch.impl.gutenberg;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;


public class ParseArchive extends CorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(ParseArchive.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
		
	public ParseArchive() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws IOException {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		setThumbnail(
				String.format(
						getData().getProperties().get("oop_DocThumbnail").asText(),
						inputStepItem.get("ebookNumber").asText(),
						inputStepItem.get("ebookNumber").asText()
				),
				outputStepItem
		);
		setLink( 
				String.format(
						getData().getProperties().get("ebookLink").asText(),
						inputStepItem.get("ebookNumber").asText(),
						inputStepItem.get("ebookNumber").asText(),
						inputStepItem.get("ebookNumber").asText()
				),
				outputStepItem
		);
		retval.add(outputStepItem);
		return retval;
	}
}
