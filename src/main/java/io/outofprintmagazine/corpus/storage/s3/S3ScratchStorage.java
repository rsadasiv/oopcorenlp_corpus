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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.IScratchStorage;
import io.outofprintmagazine.util.IParameterStore;

public class S3ScratchStorage implements IScratchStorage {

	//extends FileCorpora
	//Path=//
	//Bucket=

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(S3ScratchStorage.class);
	
	protected Logger getLogger() {
		return logger;
	}
	
	public S3ScratchStorage() {
		super();
		mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		mapper.configure(com.fasterxml.jackson.core.JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
	}
	
	public S3ScratchStorage(IParameterStore parameterStore) {
		this();
		this.setParameterStore(parameterStore);
	}
	
	protected ObjectMapper getMapper() {
		return mapper; 
	}
	
	protected ObjectMapper mapper;
	
	private IParameterStore parameterStore;
	
	public IParameterStore getParameterStore() {
		return parameterStore;
	}
	
	@Override
    public void setParameterStore(IParameterStore parameterStore) {
		this.parameterStore = parameterStore;
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
				+ "BatchItemProperties.json"
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
				+ "BatchProperties.json"
		);
	}
	
	@Override
	public String getScratchFilePath(String stagingBatchName, String stagingBatchItemName, String scratchFileName) throws Exception {
		return (
				stagingBatchName
				+ "/"
				+ stagingBatchItemName
				+ "/"
				+ scratchFileName
		);
	}


	@Override
	public String storeScratchFileString(String corpus, String scratchFileName, String in) throws Exception {
		return storeScratchFileObject(corpus, scratchFileName, in);
		//storeScratchFileProperties(corpus, stagingBatchName, scratchFileName, properties);
	}
	
	protected String storeScratchFileObject(String corpus, String scratchFileName, String in) throws Exception {
        Long contentLength = Long.valueOf(in.getBytes(StandardCharsets.UTF_8.name()).length);
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(contentLength);
		//metadata.setContentType(properties.get("mimeType").asText("application/json"));
		metadata.setContentEncoding(StandardCharsets.UTF_8.name());
		S3Utils.getInstance(getParameterStore()).getS3Client().putObject(
				new PutObjectRequest(
						getParameterStore().getProperty("s3_Bucket"),
						getCorpusStagingBatchScratchFilePath(
								corpus, 
								scratchFileName
						),
						IOUtils.toInputStream(in, StandardCharsets.UTF_8.name()),
						metadata
				)
		);
		return scratchFileName;

	}

	//TODO - Plain text?
	@Override
	public String storeScratchFileStream(String corpus, String scratchFileName, InputStream in) throws Exception {
		//do I need to buffer this in a file?
		File f = null;
		FileOutputStream fout = null;
		
		try {
			f = File.createTempFile(scratchFileName, scratchFileName.substring(trimFileExtension(scratchFileName).length()+1));
			fout = new FileOutputStream(f);
			IOUtils.copy(in,fout);
			in.close();
			fout.flush();

			S3Utils.getInstance(getParameterStore()).getS3Client().putObject(
					new PutObjectRequest(
							getParameterStore().getProperty("s3_Bucket"),
							getCorpusStagingBatchScratchFilePath(
									corpus, 
									scratchFileName
							),
							f
					)
			);
		}
		finally {
			if (fout != null) {
				fout.close();
				fout = null;
			}
			if (f != null) {
				f.delete();
				f = null;
			}
		}
		return scratchFileName;
		
	}
	
	@Override
	public String storeScratchFileObject(String corpus, String scratchFilePath, ObjectNode in) throws Exception {
		return storeJsonFile(corpus, scratchFilePath, in);
	}
		
	
	
	public String storeJsonFile(String corpus, String scratchFileName, ObjectNode in) throws Exception {
		ObjectWriter writer = getMapper().writer(new DefaultPrettyPrinter());
		String buf = writer.writeValueAsString(in);
		return storeScratchFileObject(corpus, scratchFileName, buf);
		//return storeScratchFileProperties(corpus, stagingBatchName, scratchFileName, properties);
	}

	
	public String storeJsonFileStream(String corpus, String scratchFileName, InputStream in) throws Exception {
		return storeScratchFileObject(corpus, scratchFileName, IOUtils.toString(in, StandardCharsets.UTF_8.name()));
		//return storeScratchFileProperties(corpus, stagingBatchName, scratchFileName, properties);
	}

	@Override
	public InputStream getScratchFileStream(String corpus, String scratchFileName) throws Exception {
		try {
			return S3Utils.getInstance(getParameterStore()).getS3Client().getObject(
					new GetObjectRequest(
							getParameterStore().getProperty("s3_Bucket"), 
							getCorpusStagingBatchScratchFilePath(corpus, scratchFileName)
					)
			).getObjectContent();
		}
		catch (AmazonS3Exception s3e) {
			getLogger().error(s3e);
			Thread.sleep(1000);
			return S3Utils.getInstance(getParameterStore()).getS3Client().getObject(
					new GetObjectRequest(
							getParameterStore().getProperty("s3_Bucket"), 
							getCorpusStagingBatchScratchFilePath(corpus, scratchFileName)
					)
			).getObjectContent();
		}

	}

	@Override
	public String getScratchFileString(String corpus, String scratchFileName) throws Exception {
		//getLogger().debug(String.format("s3:get %s %s", corpus, scratchFileName));
		try {
			return IOUtils.toString(
					S3Utils.getInstance(getParameterStore()).getS3Client().getObject(
							new GetObjectRequest(
									getParameterStore().getProperty("s3_Bucket"), 
									getCorpusStagingBatchScratchFilePath(corpus, scratchFileName)
							)
					).getObjectContent(),
					StandardCharsets.UTF_8.name()
			);
		}
		catch (AmazonS3Exception s3e) {
			getLogger().error(s3e);
			Thread.sleep(1000);
			return IOUtils.toString(
					S3Utils.getInstance(getParameterStore()).getS3Client().getObject(
							new GetObjectRequest(
									getParameterStore().getProperty("s3_Bucket"), 
									getCorpusStagingBatchScratchFilePath(corpus, scratchFileName)
							)
					).getObjectContent(),
					StandardCharsets.UTF_8.name()
			);
		}
	}

	@Override
	public String getFileNameFromPath(String scratchFilePath) {
		String[] paths = scratchFilePath.split(Pattern.quote("/"));
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
