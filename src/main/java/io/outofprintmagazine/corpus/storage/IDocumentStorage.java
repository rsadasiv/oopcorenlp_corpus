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

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.util.IParameterStore;

public interface IDocumentStorage {
	
    void setParameterStore(IParameterStore parameterStore);

	void storeCoreNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	ObjectNode getCoreNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	void storeOOPNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	ObjectNode getOOPNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	void storeOOPAggregates(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	ObjectNode getOOPAggregates(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	public ObjectNode getCorpusAggregateScores(String corpus) throws Exception;
	
	public ObjectNode getCorpusIDFScores(String corpus) throws Exception;
	
	void storeOOPZScores(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	void storeOOPTfidfScores(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	public ObjectNode getCorpusMyersBriggsAggregateScores(String corpus) throws Exception;
	
	void storeOOPMyersBriggsScores(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	void storeAsciiText(String corpus, String stagingBatchName, String scratchFileName, String in) throws Exception;
	
	String getAsciiText(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
	void storePipelineInfo(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception;
	
	ObjectNode getPipelineInfo(String corpus, String stagingBatchName, String scratchFileName) throws Exception;
	
}
