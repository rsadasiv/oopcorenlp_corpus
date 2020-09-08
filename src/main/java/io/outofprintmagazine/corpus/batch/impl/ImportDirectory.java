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

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;

public class ImportDirectory extends CorpusBatchStep implements ICorpusBatchStep {

	private static final Logger logger = LogManager.getLogger(ImportDirectory.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}

	public ImportDirectory() {
		super();
	}
	
	@Override
	public ObjectNode getDefaultProperties() {
		ObjectNode properties = getMapper().createObjectNode();
		properties.put("directory", ".");
		properties.put("fileSuffix", ".txt");
		return properties;
	}
	
	@Override
	public ArrayNode run(ArrayNode input) {
		for (File f : listDirectory()) {
			try {
		    	ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		    	ObjectNode inputStepItem = mapper.createObjectNode();
		    	inputStepItem.put("stagingLocation", f.getCanonicalPath());
		    	setLink(f.getName(), inputStepItem);
				File properties = new File(
						f.getCanonicalPath()
						+
						".properties"
				);
				if (properties.exists()) {
					Properties props = new Properties();
					FileInputStream in = null;
					try {
						in = new FileInputStream(properties);
						props.load(in);
						for (Entry<Object,Object> property : props.entrySet()) {
							inputStepItem.put(property.getKey().toString(), property.getValue().toString());
						}
					}
					catch (Exception e) {
						getLogger().error("No Properties file: " + f.getCanonicalPath());
					}
					finally {
						if (in != null) {
							in.close();
						}
					}
				}
				ArrayNode generatedOutput = runOne(inputStepItem);
				for (JsonNode generatedOutputItem : generatedOutput) {
					boolean foundOutputItem = false;
					for (JsonNode existingOutputItem : getData().getInput()) {
						if (existingOutputItem.equals(generatedOutputItem)) {
							foundOutputItem = true;
							break;
						}
					}
					if (!foundOutputItem) {
						getData().getOutput().add(generatedOutputItem);
					}
				}
			}
			catch (Exception ioe) {
				getLogger().error(ioe);
			}
		}
		return getData().getOutput();
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		File f = new File(outputStepItem.get("stagingLocation").asText());
        FileInputStream fout = null;
        try {
        	fout = new FileInputStream(f);
			setStorageLink(
					getStorage().storeScratchFileStream(
						getData().getCorpusId(),
						getOutputScratchFilePath(
								f.getName()
						),
						fout
					),
					outputStepItem
			);
			retval.add(outputStepItem);
        }
        catch (Exception e) {
        	if (fout != null) {
        		fout.close();
        	}
        }
		return retval;
	}
	
	class DynamicFilter implements FilenameFilter {

		private String suffix = "";
		
		public DynamicFilter(String suffix) {
			this.suffix = suffix;
		}
		
		@Override
		public boolean accept(File dir, String name) {
			return name.endsWith(suffix);
		}

	}
	
	private List<File> listDirectory() {
		return Arrays.asList(
			new File(
				getData().getProperties().get("directory").asText()
            ).listFiles(
            		new DynamicFilter(
            				getData().getProperties().get("fileSuffix").asText()
            		)
            )
		);
	}

}
