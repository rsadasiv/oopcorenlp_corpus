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

public interface IBatchStorage {
	
    void setParameterStore(IParameterStore parameterStore);

	ObjectNode listCorpora() throws Exception;
	
	void createCorpus(String corpus) throws Exception;

	ObjectNode listStagingBatches(String corpus) throws Exception;

	ObjectNode getStagingBatch(String corpus, String stagingBatchName) throws Exception;
	
	void storeStagingBatchJson(String corpus, String stagingBatchName, ObjectNode properties) throws Exception;

	void storeStagingBatchString(String corpus, String stagingBatchName, String batchContent) throws Exception;
	
}
