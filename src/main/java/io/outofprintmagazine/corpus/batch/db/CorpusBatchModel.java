package io.outofprintmagazine.corpus.batch.db;

import java.io.Serializable;
import java.util.ArrayList;

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
	
	
		
}
