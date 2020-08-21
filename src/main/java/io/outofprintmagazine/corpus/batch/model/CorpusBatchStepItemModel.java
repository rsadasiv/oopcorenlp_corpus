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
package io.outofprintmagazine.corpus.batch.model;

import java.io.Serializable;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class CorpusBatchStepItemModel implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public CorpusBatchStepItemModel() {
		super();
	}
	
	private String CorpusId;
	private String CorpusBatchId;
	private String CorpusBatchStepId;
	private String CorpusBatchStepItemId;
	private Integer CorpusBatchStepItemSequenceId;
	private ObjectNode Properties;

	
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
	
	public String getCorpusBatchStepId() {
		return CorpusBatchStepId;
	}
	
	public void setCorpusBatchStepId(String corpusBatchStepId) {
		CorpusBatchStepId = corpusBatchStepId;
	}
	
	public String getCorpusBatchStepItemId() {
		return CorpusBatchStepItemId;
	}
	
	public void setCorpusBatchStepItemId(String corpusBatchStepItemId) {
		CorpusBatchStepItemId = corpusBatchStepItemId;
	}
	
	public Integer getCorpusBatchStepItemSequenceId() {
		return CorpusBatchStepItemSequenceId;
	}
	
	public void setCorpusBatchStepItemSequenceId(Integer corpusBatchStepItemSequenceId) {
		CorpusBatchStepItemSequenceId = corpusBatchStepItemSequenceId;
	}
	
	public ObjectNode getProperties() {
		return Properties;
	}
	
	public void setProperties(ObjectNode properties) {
		Properties = properties;
	}
	


}
