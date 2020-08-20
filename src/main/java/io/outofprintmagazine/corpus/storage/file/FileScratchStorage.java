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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.ScratchStorage;
import io.outofprintmagazine.util.ParameterStore;


public class FileScratchStorage implements ScratchStorage {
	//implements CorpusStorage {

	protected ObjectMapper getMapper() {
		return mapper; 
	}
	
	protected ObjectMapper mapper;
	
	//protected String getDefaultPath() {
	//	return "C:\\Users\\rsada\\eclipse-workspace\\oopcorenlp_web\\WebContent\\Corpora\\Test";
	//}
	
	private ParameterStore parameterStore;
	
	public ParameterStore getParameterStore() {
		return parameterStore;
	}
	
	@Override
    public void setParameterStore(ParameterStore parameterStore) {
		this.parameterStore = parameterStore;
	}
	
	public FileScratchStorage() throws IOException {
		super();
		mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
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
				+ "BatchItemProperties.json"
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
		File file = new File(path);
		if (!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}
		return path;
	}
	
	protected String getCorpusStagingBatchPropertiesPath(String corpus, String stagingBatchName) throws IOException {
		return (
				getCorpusStagingBatchPath(corpus, stagingBatchName)
				+ System.getProperty("file.separator", "/") 
				+ "BatchProperties.json"
		);
	}
	
	@Override
	public String getScratchFilePath(String stagingBatchName, String stagingBatchItemName, String scratchFileName) throws Exception {
		return (
				stagingBatchName
				+ System.getProperty("file.separator", "/") 
				+ stagingBatchItemName
				+ System.getProperty("file.separator", "/") 
				+ scratchFileName
		);
	}
	
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeScratchFile(java.lang.String, java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode, java.lang.String)
	 */
	@Override
	public String storeScratchFileString(String corpus, String scratchFilePath, String in) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFilePath));
        FileOutputStream fout = null;
        try {
        	fout = new FileOutputStream(f);
	        fout.write(in.getBytes("utf-8"));
	        fout.flush();

        }
        finally {
        	if (fout != null) {
    	        fout.close();
        	}
        }
        return scratchFilePath;
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeScratchFileStream(java.lang.String, java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode, java.io.InputStream)
	 */
	@Override
	public String storeScratchFileStream(String corpus, String scratchFilePath, InputStream in) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus,  scratchFilePath));
        FileOutputStream fout = null;
        try {
        	fout = new FileOutputStream(f);
	        IOUtils.copy(in,fout);
	        fout.flush();
        }
        finally {
        	in.close();
        	if (fout != null) {
    	        fout.close();
        	}
        }
        return scratchFilePath;

	}
	
	@Override
	public String storeScratchFileObject(String corpus, String scratchFilePath, ObjectNode in) throws Exception {
		return storeJsonFile(corpus, scratchFilePath, in);
	}
	
	public String storeJsonFile(String corpus, String scratchFilePath, ObjectNode in) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFilePath));
        getMapper().writeValue(f, in);
        return scratchFilePath;
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeScratchFileStream(java.lang.String, java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode, java.io.InputStream)
	 */
	
	public String storeJsonFileStream(String corpus, String scratchFileName, InputStream in) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFileName));
        FileOutputStream fout = null;
        try {
        	fout = new FileOutputStream(f);
	        IOUtils.copy(in,fout);
	        fout.flush();
        }
        finally {
	        in.close();
	        if (fout != null) {
		        fout.close();
	        }
        }
        return scratchFileName;
	}
	
	//TODO - close stream
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#getScratchFileStream(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public InputStream getScratchFileStream(String corpus, String scratchFileName) throws Exception {
        File f = new File(getCorpusStagingBatchScratchFilePath(corpus, scratchFileName));
        FileInputStream fin = new FileInputStream(f);
        return fin;
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#getScratchFileString(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public String getScratchFileString(String corpus, String scratchFileName) throws Exception {	
	    return IOUtils.toString(
	    		getScratchFileStream(corpus, scratchFileName),
	    		"utf-8"
	    );
	}

	@Override
	public String getFileNameFromPath(String scratchFilePath) {
		String[] paths = scratchFilePath.split(Pattern.quote(System.getProperty("file.separator", "/")));
		return paths[paths.length-1];
	}
	
	@Override
	public String trimFileExtension(String scratchFileName) {
		int idx = scratchFileName.lastIndexOf(".");
		if (idx < 1) {
			idx = scratchFileName.length();
		}   
		return scratchFileName.substring(0, idx);
	}



}
