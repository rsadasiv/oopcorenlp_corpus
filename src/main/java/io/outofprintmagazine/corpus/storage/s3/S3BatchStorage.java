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
package io.outofprintmagazine.corpus.storage.s3;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.IBatchStorage;
import io.outofprintmagazine.util.IParameterStore;

public class S3BatchStorage implements IBatchStorage {

	//extends FileCorpora
	//Path=//
	//Bucket=

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(S3BatchStorage.class);
	
	protected Logger getLogger() {
		return logger;
	}
	
	public S3BatchStorage() {
		super();
		mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
	}
	
	public S3BatchStorage(IParameterStore parameterStore) {
		this();
		this.setParameterStore(parameterStore);
	}

	//public static final String defaultBucket = "oop-corpora";
	
	protected ObjectMapper getMapper() {
		return mapper; 
	}
	
	protected ObjectMapper mapper;
	
	public String getDefaultPath() {
		return "Test";
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
	public ObjectNode listCorpora() throws Exception  {

		ObjectNode json = getMapper().createObjectNode();
		ArrayNode corporaNode = json.putArray("Corpora");

		for (S3ObjectSummary objectSummary: S3Utils.getInstance(getParameterStore()).getS3Client().listObjects(
				getParameterStore().getProperty("s3_Bucket"), 
				getParameterStore().getProperty("s3_Path")
			).getObjectSummaries()) {
			if (objectSummary.getKey().endsWith("/")) {
				corporaNode.add(objectSummary.getKey().substring(0, objectSummary.getKey().length() - 1));
			}
		}
		return json;
	}
	
	protected String getCorpusPath(String corpus) throws IOException {
		String path = (
				getParameterStore().getProperty("s3_Path")
				+ "/"	
				+ corpus
		);

		return path;
		
	}
	
	protected String getCorpusStagingBatchPath(String corpus, String stagingBatchName) throws IOException {
		String path = (
				getCorpusPath(corpus) 
				+ "/" 
				+ stagingBatchName
		);

		return path;
	}
	
	protected String getCorpusStagingBatchItemPath(String corpus, String stagingBatchName, String stagingBatchItemName) throws IOException {
		String path = (
				getCorpusPath(corpus) 
				+ "/" 
				+ stagingBatchName
				+ "/" 
				+ stagingBatchItemName
		);

		return path;
	}
	

	protected String getCorpusStagingBatchItemPropertiesPath(String corpus, String stagingBatchName, String stagingBatchItemName) throws IOException {
		return (
				getCorpusStagingBatchItemPath(corpus, stagingBatchName, stagingBatchItemName)
				+ "/" 
				+ stagingBatchItemName + "BatchItem.json"
		);
	}	
	
	protected String getCorpusStagingBatchScratchPath(String corpus, String stagingBatchName) throws IOException {
		String path = (
				getCorpusStagingBatchPath(corpus, stagingBatchName) 
				+ "/" 
				+ "Scratch"
		);

		return path;
	}
	
	protected String getCorpusStagingBatchScratchFilePath(String corpus, String scratchFileName) throws IOException {
		String path = (
				getCorpusPath(corpus) 
				+ "/" 
				+ scratchFileName
		);
		return path;
	}
	
	protected String getCorpusStagingBatchPropertiesPath(String corpus, String stagingBatchName) throws IOException {
		return (
				getCorpusStagingBatchPath(corpus, stagingBatchName)
				+ "/" 
				+ stagingBatchName + "Batch.json"
		);
	}
	

	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeStagingBatchJson(java.lang.String, java.lang.String, com.fasterxml.jackson.databind.node.ObjectNode)
	 */
	@Override
	public void storeStagingBatchJson(String corpus, String stagingBatchName, ObjectNode properties) throws Exception {
		storeStagingBatchString(corpus, stagingBatchName, getMapper().writeValueAsString(properties));
	}
	
	/* (non-Javadoc)
	 * @see io.outofprintmagazine.corpus.storage.CorpusStorage#storeStagingBatchString(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void storeStagingBatchString(String corpus, String stagingBatchName, String in) throws Exception {
        Long contentLength = Long.valueOf(in.getBytes(StandardCharsets.UTF_8.name()).length);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentLength);
		metadata.setContentType("application/json");
		metadata.setContentEncoding(StandardCharsets.UTF_8.name());
		S3Utils.getInstance(getParameterStore()).getS3Client().putObject(
				new PutObjectRequest(
						getParameterStore().getProperty("s3_Bucket"),
						getCorpusStagingBatchPropertiesPath(corpus, stagingBatchName),
						IOUtils.toInputStream(in, StandardCharsets.UTF_8.name()),
						metadata
				)
		);		
	}

	@Override
	public ObjectNode listStagingBatches(String corpus) throws Exception {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode corporaNode = json.putArray("Corpora");

		for (S3ObjectSummary objectSummary: S3Utils.getInstance(getParameterStore()).getS3Client().listObjects(
				getParameterStore().getProperty("s3_Bucket"), 
				getCorpusPath(corpus)
			).getObjectSummaries()) {
			if (objectSummary.getKey().endsWith("/")) {
				corporaNode.add(objectSummary.getKey().substring(0, objectSummary.getKey().length() - 1));
			}
		}
		return json;
	}

	@Override
	public ObjectNode getStagingBatch(String corpus, String stagingBatchName) throws Exception {
		return (ObjectNode) getMapper().readTree(
				S3Utils.getInstance(getParameterStore()).getS3Client().getObject(
						new GetObjectRequest(
								getParameterStore().getProperty("s3_Bucket"), 
								getCorpusStagingBatchPropertiesPath(
										corpus, 
										stagingBatchName
								)
						)
				).getObjectContent()
		);
	}
}
