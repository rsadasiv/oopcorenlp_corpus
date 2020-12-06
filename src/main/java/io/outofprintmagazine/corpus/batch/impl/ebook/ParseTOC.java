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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class ParseTOC extends CorpusBatchStep implements ICorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(ParseTOC.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
		
	public ParseTOC() {
		super();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		BufferedReader reader = null;
		try {
			List<String> tocCandidates = new ArrayList<String>();
			//Story titles make up an entire lines in ALL CAPS or Title Case
			//If there is a TOC, story titles will appear twice
			Map<String, Integer> lineRepetitions = new HashMap<String, Integer>();
			reader = new BufferedReader(
					new StringReader(
							getTextDocumentFromStorage(inputStepItem)
					)
			);
			String line = null;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (line.length() > 1 && !line.contains(".") && !line.matches("^.*\\p{Punct}$") && !line.matches("^['\"\\u201C\\u201D\\u201E\\u201F\\u2033\\u2036].*")) {
					if (line.toUpperCase().equals(line) || StringUtils.capitalize(line).equals(line)) {
						Integer lineRepetitionCount = lineRepetitions.get(line);
						if (lineRepetitionCount == null) {
							lineRepetitions.put(line, Integer.valueOf(1));
						}
						else if (lineRepetitionCount.intValue() == 1) {
							lineRepetitions.put(line, Integer.valueOf(lineRepetitionCount.intValue()+1));
							tocCandidates.add(line);
						}
						else if (lineRepetitionCount.intValue() == 2) {
							lineRepetitions.put(line, Integer.valueOf(lineRepetitionCount.intValue()+1));
							tocCandidates.remove(line);
						}
						else {
							lineRepetitions.put(line, Integer.valueOf(lineRepetitionCount.intValue()+1));
						}
					}
				}
			}
			reader.close();

			//If there is no TOC, story titles will appear once
			if (tocCandidates.size() < 2) {
				tocCandidates.clear();
				lineRepetitions = new HashMap<String, Integer>();
				reader = new BufferedReader(
						new StringReader(
								getTextDocumentFromStorage(inputStepItem)
						)
				);
				line = null;
				while ((line = reader.readLine()) != null) {
					line = line.trim();
					if (line.length() > 1 && !line.contains(".") && !line.matches("^.*\\p{Punct}$") && !line.matches("^['\"\\u201C\\u201D\\u201E\\u201F\\u2033\\u2036].*")) {
						if (line.toUpperCase().equals(line) || StringUtils.capitalize(line).equals(line)) {
							Integer lineRepetitionCount = lineRepetitions.get(line);
							if (lineRepetitionCount == null) {
								lineRepetitions.put(line, Integer.valueOf(1));
								tocCandidates.add(line);
							}
							else if (lineRepetitionCount.intValue() == 1) {
								lineRepetitions.put(line, Integer.valueOf(lineRepetitionCount.intValue()+1));
								tocCandidates.remove(line);
							}
						}
					}
				}
			}

			
			
			String tocCandidate = null;
			for (String toc : tocCandidates) {
				if (tocCandidate != null) {
					ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
					setTitle(tocCandidate, outputStepItem);
					outputStepItem.put(
						"nextTitle", 
						toc
					);
					retval.add(outputStepItem);
				}
				tocCandidate = toc;
			}
			if (tocCandidate != null) {
				ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
				setTitle(tocCandidate, outputStepItem);
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
