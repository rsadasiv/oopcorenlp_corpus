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
package io.outofprintmagazine.corpus.storage.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.IBatchStorage;
import io.outofprintmagazine.util.IParameterStore;

public class FileBatchStorage implements IBatchStorage {

	protected ObjectMapper getMapper() {
		return mapper; 
	}
	
	protected ObjectMapper mapper;
	
	public FileBatchStorage() throws IOException {
		super();
		mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);

	}
	
	private IParameterStore parameterStore;
	
	public IParameterStore getParameterStore() {
		return parameterStore;
	}
	
	@Override
    public void setParameterStore(IParameterStore parameterStore) {
		this.parameterStore = parameterStore;
	}

	@Override
	public void createCorpus(String corpus) throws Exception {
		//pass
	}
	
	@Override
	public ObjectNode listCorpora() throws Exception {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode childNodes = json.putArray("Corpora");
		
		File[] directories = new File(getParameterStore().getProperty("fileCorpus_Path")).listFiles(File::isDirectory);		
		for (int i=0;i<directories.length;i++) {
			childNodes.add(directories[i].getName());
		}
		return json;
	}
	
	protected String getCorpusPath(String corpus) throws IOException {
		String path = (
				getParameterStore().getProperty("fileCorpus_Path")
				+ System.getProperty("file.separator", "/")	
				+ corpus
		);
		File dir = new File(path);
		if (!dir.exists()) dir.mkdirs();
		return path;
		
	}
	
	protected String getCorpusStagingBatchPath(String corpus, String stagingBatchName) throws IOException {
		String path = (
				getCorpusPath(corpus) 
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchName
		);
		File dir = new File(path);
		if (!dir.exists()) dir.mkdirs();
		return path;
	}
	
	protected String getCorpusStagingBatchItemPath(String corpus, String stagingBatchName, String stagingBatchItemName) throws IOException {
		String path = (
				getCorpusPath(corpus) 
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchName
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchItemName
		);
		File dir = new File(path);
		if (!dir.exists()) dir.mkdirs();
		return path;
	}
	

	protected String getCorpusStagingBatchItemPropertiesPath(String corpus, String stagingBatchName, String stagingBatchItemName) throws IOException {
		return (
				getCorpusStagingBatchItemPath(corpus, stagingBatchName, stagingBatchItemName)
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchItemName + "BatchItem.json"
		);
	}	
	
	protected String getCorpusStagingBatchScratchPath(String corpus, String stagingBatchName) throws IOException {
		String path = (
				getCorpusStagingBatchPath(corpus, stagingBatchName) 
				+ System.getProperty("file.separator", "/") 
				+ "Scratch"
		);
		File dir = new File(path);
		if (!dir.exists()) dir.mkdirs();
		return path;
	}
	
	protected String getCorpusStagingBatchScratchFilePath(String corpus, String scratchFilePath) throws IOException {
		String path = (
				getCorpusPath(corpus) 
				+ System.getProperty("file.separator", "/") 
				+ scratchFilePath
		);
		File dir = new File(path);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return path;
	}
	
	protected String getCorpusStagingBatchPropertiesPath(String corpus, String stagingBatchName) throws IOException {
		return (
				getCorpusStagingBatchPath(corpus, stagingBatchName)
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchName + "Batch.json"
		);
	}
		
	@Override
	public void storeStagingBatchJson(String corpus, String stagingBatchName, ObjectNode properties) throws Exception {
		ObjectWriter writer = getMapper().writer(new DefaultPrettyPrinter());
		writer.writeValue(
				new File(
						getCorpusStagingBatchPropertiesPath(
								corpus, 
								stagingBatchName
						)
				), 
				properties
		);
	}
	
	@Override
	public void storeStagingBatchString(String corpus, String stagingBatchName, String batchContent) throws Exception {
        File f = new File(getCorpusStagingBatchPropertiesPath(corpus, stagingBatchName));
        FileOutputStream fout = null;
        try {
        	fout = new FileOutputStream(f);
        	fout.write(batchContent.getBytes());
        	fout.flush();
        }
        finally {
        	if (fout != null) {
        		fout.close();
        	}
        }
	}
	
	@Override
	public ObjectNode listStagingBatches(String corpus) throws Exception {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode childNodes = json.putArray("StagingBatches");
		
		File[] directories = new File(getCorpusPath(corpus)).listFiles(File::isDirectory);		
		for (int i=0;i<directories.length;i++) {
			childNodes.add(directories[i].getName());
		}
		return json;
	}
	
	@Override
	public ObjectNode getStagingBatch(String corpus, String stagingBatchName) throws Exception {
		return (ObjectNode) getMapper().readTree(
				new File(
						getCorpusStagingBatchPropertiesPath(
								corpus, 
								stagingBatchName
						)
				)
		);
	}
	
	public void storeStagingBatchItem(String corpus, String stagingBatchName, String stagingBatchItemName, ObjectNode properties) throws Exception {
		ObjectWriter writer = getMapper().writer(new DefaultPrettyPrinter());
		writer.writeValue(
				new File(
						getCorpusStagingBatchItemPropertiesPath(
								corpus, 
								stagingBatchName,
								stagingBatchItemName
						)
				), 
				properties
		);
	}
	
	
}
