package io.outofprintmagazine.corpus.batch.db;

import java.io.Serializable;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CorpusBatchStepModel implements Serializable, Comparable<CorpusBatchStepModel> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public CorpusBatchStepModel() {
		super();
	}
	
	private String CorpusId;
	private String CorpusBatchId;
	private String CorpusBatchStepId;
	private Integer CorpusBatchStepSequenceId;
	private String CorpusBatchStepClass;
	private ObjectNode Properties;
	private ArrayNode Input;
	private ArrayNode Output;

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
	
	public Integer getCorpusBatchStepSequenceId() {
		return CorpusBatchStepSequenceId;
	}
	
	public void setCorpusBatchStepSequenceId(Integer corpusBatchStepSequenceId) {
		CorpusBatchStepSequenceId = corpusBatchStepSequenceId;
	}
	
	public String getCorpusBatchStepClass() {
		return CorpusBatchStepClass;
	}
	
	public void setCorpusBatchStepClass(String corpusBatchStepClass) {
		CorpusBatchStepClass = corpusBatchStepClass;
	}
	
	public ObjectNode getProperties() {
		return Properties;
	}
	
	public void setProperties(ObjectNode properties) {
		Properties = properties;
	}
	
	public ArrayNode getInput() {
		return Input;
	}
	
	public void setInput(ArrayNode input) {
		Input = input;
	}
	
	public ArrayNode getOutput() {
		return Output;
	}
	
	public void setOutput(ArrayNode output) {
		Output = output;
	}

	public int compareTo(CorpusBatchStepModel o) {
		return getCorpusBatchStepSequenceId().compareTo(o.getCorpusBatchStepSequenceId());
	}
}
