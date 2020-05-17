package io.outofprintmagazine.corpus.batch.db;

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
