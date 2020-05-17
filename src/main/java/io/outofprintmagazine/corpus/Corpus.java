package io.outofprintmagazine.corpus;

import java.io.Serializable;

public class Corpus implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Corpus() {
		super();
	}
	
	private String CorpusId;

	
	public String getCorpusId() {
		return CorpusId;
	}
	
	public void setCorpusId(String corpusId) {
		CorpusId = corpusId;
	}
	
}
