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
package io.outofprintmagazine.corpus.batch.db;

import java.io.Serializable;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class CorpusBatchModel implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	public CorpusBatchModel() {
		super();
	}
	
	private String CorpusId;
	private String CorpusBatchId;
	private String BatchStorageClass;
	private String ScratchStorageClass;
	private String ParameterStoreClass;
	private ObjectNode Properties;
	
	private ArrayList<CorpusBatchStepModel> CorpusBatchSteps = new ArrayList<CorpusBatchStepModel>();

	
	public String getCorpusId() {
		return CorpusId;
	}
	
	public void setCorpusId(String corpusId) {
		CorpusId = corpusId;
	}
	
	public String getCorpusBatchId() {
		return CorpusBatchId;
	}
	
	public void setCorpusBatchId(String corpusBatchId) {
		CorpusBatchId = corpusBatchId;
	}

	public ArrayList<CorpusBatchStepModel> getCorpusBatchSteps() {
		return CorpusBatchSteps;
	}

	public void setCorpusBatchSteps(ArrayList<CorpusBatchStepModel> corpusBatchSteps) {
		CorpusBatchSteps = corpusBatchSteps;
	}

	public String getBatchStorageClass() {
		return BatchStorageClass;
	}

	public void setBatchStorageClass(String batchStorageClass) {
		BatchStorageClass = batchStorageClass;
	}
	
	public String getScratchStorageClass() {
		return ScratchStorageClass;
	}

	public void setScratchStorageClass(String scratchStorageClass) {
		ScratchStorageClass = scratchStorageClass;
	}

	public String getParameterStoreClass() {
		return ParameterStoreClass;
	}

	public void setParameterStoreClass(String parameterStoreClass) {
		ParameterStoreClass = parameterStoreClass;
	}

	public ObjectNode getProperties() {
		return Properties;
	}

	public void setProperties(ObjectNode properties) {
		Properties = properties;
	}	
}
