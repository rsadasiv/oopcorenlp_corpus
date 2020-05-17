package io.outofprintmagazine.corpus.storage;

import java.io.InputStream;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface ScratchStorage {

	String getFileNameFromPath(String scratchFilePath);
	
	String trimFileExtension(String scratchFileName);
	
	String getScratchFilePath(String stagingBatchName, String stagingBatchStepName, String scratchFileName) throws Exception;
	
	String storeScratchFileString(String corpus, String scratchFilePath, String in) throws Exception;

	String storeScratchFileStream(String corpus, String scratchFilePath, InputStream in) throws Exception;
	
	String storeScratchFileObject(String corpus, String scratchFilePath, ObjectNode in) throws Exception;

	String getScratchFileString(String corpus, String scratchFilePath) throws Exception;
	
	InputStream getScratchFileStream(String corpus, String scratchFilePath) throws Exception;

	ObjectNode getScratchFileProperties(String corpus, String scratchFilePath) throws Exception;
	
}