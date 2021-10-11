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
package io.outofprintmagazine.corpus.batch.impl.ebook;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class ParseTOCRegex extends CorpusBatchStep implements ICorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(ParseTOCRegex.class);
	
	protected Pattern pattern = null;
	
	
	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
		
	public ParseTOCRegex() {
		super();
	}
	
	@Override
	public ObjectNode getDefaultProperties() {
		ObjectNode properties = getMapper().createObjectNode();
		properties.put("TOCRegex", "^\\* (.*)$");
		properties.put("upperCase", "false");
		return properties;
	}
	
	@Override
	public ArrayNode run(ArrayNode input) {
		pattern = Pattern.compile(getData().getProperties().get("TOCRegex").asText());
		return super.run(input);
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		BufferedReader reader = null;
		try {
			List<String> tocCandidates = new ArrayList<String>();
			reader = new BufferedReader(
					new StringReader(
							getTextDocumentFromStorage(inputStepItem)
					)
			);
			String line = null;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
		        Matcher matcher = pattern.matcher(line);
		        if (matcher.find()) {
		        	tocCandidates.add(matcher.group(1));
		        }
			}

			String tocCandidate = null;
			for (String toc : tocCandidates) {
				if (tocCandidate != null) {
					ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		        	if (getData().getProperties().get("upperCase").asBoolean(false)) {
		        		setTitle(tocCandidate.toUpperCase(), outputStepItem);
						outputStepItem.put(
								"nextTitle", 
								toc.toUpperCase()
							);
		        	}
		        	else {
		        		setTitle(tocCandidate, outputStepItem);
		        		outputStepItem.put(
		        				"nextTitle", 
		        				toc
		        		);
		        	}
					retval.add(outputStepItem);
				}
				tocCandidate = toc;
			}
			if (tocCandidate != null) {
				ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
	        	if (getData().getProperties().get("upperCase").asBoolean(false)) {
	        		setTitle(tocCandidate.toUpperCase(), outputStepItem);
	        	}
	        	else {
	        		setTitle(tocCandidate, outputStepItem);
	        	}
				retval.add(outputStepItem);
			}
			return retval;
		}
		finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
}
