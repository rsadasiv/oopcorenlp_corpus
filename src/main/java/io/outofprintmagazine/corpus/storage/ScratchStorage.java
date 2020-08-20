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
package io.outofprintmagazine.corpus.storage;

import java.io.InputStream;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.util.ParameterStore;

public interface ScratchStorage {
	
    void setParameterStore(ParameterStore parameterStore);

	String getFileNameFromPath(String scratchFilePath);
	
	String trimFileExtension(String scratchFileName);
	
	String getScratchFilePath(String stagingBatchName, String stagingBatchStepName, String scratchFileName) throws Exception;
	
	String storeScratchFileString(String corpus, String scratchFilePath, String in) throws Exception;

	String storeScratchFileStream(String corpus, String scratchFilePath, InputStream in) throws Exception;
	
	String storeScratchFileObject(String corpus, String scratchFilePath, ObjectNode in) throws Exception;

	String getScratchFileString(String corpus, String scratchFilePath) throws Exception;
	
	InputStream getScratchFileStream(String corpus, String scratchFilePath) throws Exception;
	
}
