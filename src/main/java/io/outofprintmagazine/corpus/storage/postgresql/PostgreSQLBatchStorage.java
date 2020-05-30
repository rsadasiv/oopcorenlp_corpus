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
package io.outofprintmagazine.corpus.storage.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatch;
import io.outofprintmagazine.corpus.batch.db.CorpusBatchModel;
import io.outofprintmagazine.corpus.batch.db.CorpusBatchStepModel;
import io.outofprintmagazine.corpus.storage.BatchStorage;
import io.outofprintmagazine.util.ParameterStore;


public class PostgreSQLBatchStorage implements BatchStorage {

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(PostgreSQLBatchStorage.class);
	
	protected Logger getLogger() {
		return logger;
	}

	public PostgreSQLBatchStorage() {
		super();
	}
	
	public PostgreSQLBatchStorage(ParameterStore parameterStore) {
		this();
		this.setParameterStore(parameterStore);
	}
	
	private ObjectMapper mapper = new ObjectMapper();
	

	protected ObjectMapper getMapper() {
		return mapper;
	}
	
	private ParameterStore parameterStore;
	
	public ParameterStore getParameterStore() {
		return parameterStore;
	}
	
	@Override
    public void setParameterStore(ParameterStore parameterStore) {
		this.parameterStore = parameterStore;
	}
	



	@Override
	public ObjectNode listCorpora() throws Exception {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode corporaNode = json.putArray("Corpora");
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet cursor = null;
        try {
		    conn = PostgreSQLUtils.getInstance(getParameterStore()).getClient();
		    String sql = "SELECT schema_name FROM information_schema.schemata where schema_name not in ('pg_catalog', 'information_schema', 'public')"; 
		    pstmt = conn.prepareStatement(sql);
		    cursor = pstmt.executeQuery();
		    while(cursor.next()) {
		    	corporaNode.add(cursor.getString(1));
		    }
        }
        finally {
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(cursor);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(pstmt);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(conn);
        }
		return json;
	}
	
	@Override
	public void createCorpus(String corpus) throws Exception {
		Connection conn = null;
		Statement stmt = null;
        try {
		    conn = PostgreSQLUtils.getInstance(getParameterStore()).getClient();
//		    String sql = "create schema %s if not exists authorization %s";
//		    stmt = conn.createStatement();
//		    stmt.execute(String.format(sql, corpus, PostgreSQLUtils.getInstance().getLoginRole()));
//		    stmt.close();
		    conn.setSchema(corpus);
		    stmt = conn.createStatement();
		    String[] ddl = 
		    		IOUtils.toString(
		    				getClass().getClassLoader().getResourceAsStream(
		    						"io/outofprintmagazine/corpus/storage/db/create_staging_schema.sql"
		    				), 
		    				"UTF-8"
		    		).split(";");
		    for (int i=0;i<ddl.length;i++) {
		    	stmt.execute(ddl[i]);
		    }
		    
		    ddl = 
		    		IOUtils.toString(
		    				getClass().getClassLoader().getResourceAsStream(
		    						"io/outofprintmagazine/corpus/storage/db/create_oopnlp_schema.sql"
		    				), 
		    				"UTF-8"
		    		).split(";");
		    for (int i=0;i<ddl.length;i++) {
		    	stmt.execute(ddl[i]);
		    }
		    
		    String functionCode = 
		    		IOUtils.toString(
		    				getClass().getClassLoader().getResourceAsStream(
		    						"io/outofprintmagazine/corpus/storage/db/create_aggregate_functions.sql"
		    				), 
		    				"UTF-8"
		    		);
		    stmt.execute(functionCode);
		    
		    stmt.close();
        }
        finally {
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(stmt);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(conn);
        }
	}


	public void createStagingBatch(String corpus, String stagingBatchName, CorpusBatch batch) throws Exception {
		createCorpus(corpus);
		Connection conn = null;
		PreparedStatement pstmt = null;
        try {
		    conn = PostgreSQLUtils.getInstance(getParameterStore()).getClient(corpus);
		    String sql = "insert into staging_batches(corpus_batch_id, name, description, json_data) values (?, ?, ?, ?) ON CONFLICT DO NOTHING";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, stagingBatchName);
		    pstmt.setString(2, stagingBatchName);
		    pstmt.setString(3, stagingBatchName);
	    	PGobject itemProperties = new PGobject();
	    	itemProperties.setType("jsonb");
	    	itemProperties.setValue(getMapper().writeValueAsString(batch));
	    	pstmt.setObject(4, itemProperties);
		    pstmt.executeUpdate();
		    pstmt.close();
        }
        finally {
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(pstmt);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(conn);
        }
	}
	

	public void storeStagingBatchObject(String corpus, String stagingBatchName, CorpusBatch batch)
			throws Exception {
		createStagingBatch(corpus, stagingBatchName, batch);
		Connection conn = null;
		PreparedStatement batchItemsInsert = null;
		PreparedStatement batchItemsInputInsert = null;
		PreparedStatement batchItemsOutputInsert = null;
        try {
		    conn = PostgreSQLUtils.getInstance(getParameterStore()).getClient(corpus);
		    String sql = "insert into staging_batch_items("
    		+ "corpus_batch_id"
    		+ ", corpus_batch_step_id"
    		+ ", corpus_batch_step_sequence_id"
    		+ ", corpus_batch_step_class"
    		+ ", properties"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ", ?"
    		+ ", ?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    batchItemsInsert = conn.prepareStatement(sql);
		    sql = "insert into staging_batch_item_input("
    		+ "corpus_batch_id"
    		+ ", corpus_batch_step_id"
    		+ ", corpus_batch_step_sequence_id"
    		+ ", corpus_batch_step_class"
    		+ ", staging_batch_item_input_id"
    		+ ", property_name"
    		+ ", property_value) "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ", ?"
    		+ ", ?"
    		+ ", ?"
    		+ ", ?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    batchItemsInputInsert = conn.prepareStatement(sql);
		    sql = "insert into staging_batch_item_output("
    		+ "corpus_batch_id"
    		+ ", corpus_batch_step_id"
    		+ ", corpus_batch_step_sequence_id"
    		+ ", corpus_batch_step_class"
    		+ ", staging_batch_item_output_id"
    		+ ", property_name"
    		+ ", property_value) "
    		+ " values ("
    		+ " ?"
    		+ ", ?"
    		+ ", ?"
    		+ ", ?"
    		+ ", ?"
    		+ ", ?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    batchItemsOutputInsert = conn.prepareStatement(sql);

		    for (CorpusBatchStepModel batchStep : batch.getData().getCorpusBatchSteps()) {

		    	batchItemsInsert.setString(1, batchStep.getCorpusBatchId());
		    	batchItemsInsert.setString(2, batchStep.getCorpusBatchStepId());
		    	batchItemsInsert.setInt(3, batchStep.getCorpusBatchStepSequenceId());
		    	batchItemsInsert.setString(4, batchStep.getCorpusBatchStepClass());
		    	PGobject itemProperties = new PGobject();
		    	itemProperties.setType("json");
		    	itemProperties.setValue(getMapper().writeValueAsString(batchStep.getProperties()));
		    	batchItemsInsert.setObject(5, itemProperties);

		    	batchItemsInsert.executeUpdate();
	    		int inputId = 0;
		    	for (JsonNode batchStepInputItem : batchStep.getInput()) {
		    		Iterator<String> batchStepInputItemPropertiesIter = batchStepInputItem.fieldNames();
		    		while (batchStepInputItemPropertiesIter.hasNext()) {
		    			String propertyName = batchStepInputItemPropertiesIter.next();
		    			String propertyValue = batchStepInputItem.get(propertyName).asText();
				    	batchItemsInputInsert.setString(1, batchStep.getCorpusBatchId());
				    	batchItemsInputInsert.setString(2, batchStep.getCorpusBatchStepId());
				    	batchItemsInputInsert.setInt(3, batchStep.getCorpusBatchStepSequenceId());
				    	batchItemsInputInsert.setString(4, batchStep.getCorpusBatchStepClass());
				    	batchItemsInputInsert.setInt(5, inputId);
				    	batchItemsInputInsert.setString(6, propertyName);
				    	batchItemsInputInsert.setString(7, propertyValue);
				    	batchItemsInputInsert.executeUpdate();
		    		}
		    		inputId++;
		    	}
		    	int outputId = 0;
		    	for (JsonNode batchStepOutputItem : batchStep.getOutput()) {
		    		Iterator<String> batchStepOutputItemPropertiesIter = batchStepOutputItem.fieldNames();
		    		while (batchStepOutputItemPropertiesIter.hasNext()) {
		    			String propertyName = batchStepOutputItemPropertiesIter.next();
		    			String propertyValue = batchStepOutputItem.get(propertyName).asText();
				    	batchItemsOutputInsert.setString(1, batchStep.getCorpusBatchId());
				    	batchItemsOutputInsert.setString(2, batchStep.getCorpusBatchStepId());
				    	batchItemsOutputInsert.setInt(3, batchStep.getCorpusBatchStepSequenceId());
				    	batchItemsOutputInsert.setString(4, batchStep.getCorpusBatchStepClass());
				    	batchItemsOutputInsert.setInt(5, outputId);
				    	batchItemsOutputInsert.setString(6, propertyName);
				    	batchItemsOutputInsert.setString(7, propertyValue);
				    	batchItemsOutputInsert.executeUpdate();
		    		}
		    		outputId++;
		    	}
		    	
		    }
		    batchItemsInputInsert.close();
		    batchItemsOutputInsert.close();
		    batchItemsInsert.close();
        }
        finally {
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(batchItemsOutputInsert);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(batchItemsInputInsert);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(batchItemsInsert);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(conn);
        }
	}
	
	@Override
	public void storeStagingBatchJson(String corpus, String stagingBatchName, ObjectNode properties) throws Exception {
	    storeStagingBatchObject(corpus, stagingBatchName, CorpusBatch.buildFromJson(corpus, properties));
	}

	@Override
	public void storeStagingBatchString(String corpus, String stagingBatchName, String batchContent) throws Exception {
		storeStagingBatchObject(corpus, stagingBatchName, CorpusBatch.buildFromString(corpus, batchContent));
	}

	@Override
	public ObjectNode listStagingBatches(String corpus) throws Exception  {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode corporaNode = json.putArray("staging_batches");
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet cursor = null;
        try {
		    conn = PostgreSQLUtils.getInstance(getParameterStore()).getClient();
		    String sql = "SELECT corpus_batch_id from staging_batches order by corpus_batch_id"; 
		    pstmt = conn.prepareStatement(sql);
		    cursor = pstmt.executeQuery();
		    while(cursor.next()) {
		    	corporaNode.add(cursor.getString(1));
		    }
        }
        finally {
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(cursor);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(pstmt);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(conn);
        }
		return json;
	}
	
	@Override
	public ObjectNode getStagingBatch(String corpus, String stagingBatchName) throws Exception {
		return getMapper().valueToTree(
				getStagingBatchObject(corpus, stagingBatchName)
		);	
	}
	
	public CorpusBatchModel getStagingBatchObject(String corpus, String stagingBatchName) throws Exception {
		CorpusBatchModel retval = new CorpusBatchModel();
		retval.setCorpusId(corpus);
		retval.setCorpusBatchId(stagingBatchName);
		ArrayList<CorpusBatchStepModel> steps = new ArrayList<CorpusBatchStepModel>();
		retval.setCorpusBatchSteps(steps);
		Connection conn = null;
		PreparedStatement batchItemsSelect = null;
		PreparedStatement batchItemsInputSelect = null;
		PreparedStatement batchItemsOutputSelect = null;
		ResultSet batchItemsCursor = null;
		ResultSet batchItemsInputCursor = null;
		ResultSet batchItemsOutputCursor = null;
        try {
		    conn = PostgreSQLUtils.getInstance(getParameterStore()).getClient(corpus);
		    String sql = "select "
		    		+ "corpus_batch_id"
		    		+ ", corpus_batch_step_id"
		    		+ ", corpus_batch_step_sequence_id"
		    		+ ", corpus_batch_step_class"
		    		+ ", properties "
		    		+ "from staging_batch_items "
		    		+ "where corpus_batch_id = ? "
		    		+ "order by corpus_batch_step_sequence_id asc";
		    batchItemsSelect = conn.prepareStatement(sql);
		    sql = "select "
    		+ "staging_batch_item_input_id"
    		+ ", property_name"
    		+ ", property_value"   		
    		+ "from staging_batch_item_input "
    		+ "where corpus_batch_id = ? "
    		+ "and corpus_batch_step_id = ? "
    		+ "and corpus_batch_step_sequence_id = ? "
    		+ "and corpus_batch_step_class = ? "
    		+ "order by staging_batch_item_input_id asc";
		    batchItemsInputSelect = conn.prepareStatement(sql);
		    sql = "select "
    		+ "staging_batch_item_output_id"
    		+ ", property_name"
    		+ ", property_value"  
    		+ "from staging_batch_item_output "
    		+ "where corpus_batch_id = ? "
    		+ "and corpus_batch_step_id = ? "
    		+ "and corpus_batch_step_sequence_id = ? "
    		+ "and corpus_batch_step_class = ? "
     		+ "order by staging_batch_item_output_id asc";
		    batchItemsOutputSelect = conn.prepareStatement(sql);
		    
		    batchItemsSelect.setString(1, stagingBatchName);
		    batchItemsCursor = batchItemsSelect.executeQuery();
		    while (batchItemsCursor.next()) {
		    	CorpusBatchStepModel corpusBatchStepModel = new CorpusBatchStepModel();
		    	corpusBatchStepModel.setCorpusBatchId(batchItemsCursor.getString("corpus_batch_id"));
		    	corpusBatchStepModel.setCorpusBatchStepId(batchItemsCursor.getString("corpus_batch_step_id"));
		    	corpusBatchStepModel.setCorpusBatchStepSequenceId(batchItemsCursor.getInt("corpus_batch_step_sequence_id"));
		    	corpusBatchStepModel.setCorpusBatchStepClass(batchItemsCursor.getString("corpus_batch_step_class"));
		    	corpusBatchStepModel.setProperties(
		    			(ObjectNode) getMapper().readTree(batchItemsCursor.getString("properties"))
		    	);
		    	steps.add(corpusBatchStepModel);
		    	
			    batchItemsInputSelect.setString(1, corpusBatchStepModel.getCorpusBatchId());
			    batchItemsInputSelect.setString(2, corpusBatchStepModel.getCorpusBatchStepId());
			    batchItemsInputSelect.setInt(3, corpusBatchStepModel.getCorpusBatchStepSequenceId());
			    batchItemsInputSelect.setString(4, corpusBatchStepModel.getCorpusBatchStepClass());
			    getLogger().debug(batchItemsInputSelect.toString());
			    batchItemsInputCursor = batchItemsInputSelect.executeQuery();
			    ArrayNode input = getMapper().createArrayNode();
			    int currentInputItem = 0;
			    ObjectNode inputProperties = getMapper().createObjectNode();
			    while (batchItemsInputCursor.next()) {
			    	if (currentInputItem != batchItemsInputCursor.getInt("staging_batch_item_input_id")) {
			    		input.add(inputProperties);
			    		currentInputItem = batchItemsInputCursor.getInt("staging_batch_item_input_id");
			    		inputProperties = getMapper().createObjectNode();
			    	}
			    	else {
			    		inputProperties.put(batchItemsInputCursor.getString("property_name"), batchItemsInputCursor.getString("property_value"));
			    	}

			    }
			    corpusBatchStepModel.setInput(input);
			    batchItemsInputCursor.close();

			    batchItemsOutputSelect.setString(1, corpusBatchStepModel.getCorpusBatchId());
			    batchItemsOutputSelect.setString(2, corpusBatchStepModel.getCorpusBatchStepId());
			    batchItemsOutputSelect.setInt(3, corpusBatchStepModel.getCorpusBatchStepSequenceId());
			    batchItemsOutputSelect.setString(4, corpusBatchStepModel.getCorpusBatchStepClass());
			    batchItemsOutputCursor = batchItemsOutputSelect.executeQuery();
			    ArrayNode output = getMapper().createArrayNode();
			    int currentOutputItem = 0;
			    ObjectNode outputProperties = getMapper().createObjectNode();
			    while (batchItemsOutputCursor.next()) {
			    	if (currentOutputItem != batchItemsOutputCursor.getInt("staging_batch_item_output_id")) {
			    		output.add(outputProperties);
			    		currentOutputItem = batchItemsOutputCursor.getInt("staging_batch_item_output_id");
			    		outputProperties = getMapper().createObjectNode();
			    	}
			    	else {
			    		outputProperties.put(batchItemsOutputCursor.getString("property_name"), batchItemsOutputCursor.getString("property_value"));
			    	}

			    }
			    corpusBatchStepModel.setOutput(output);
			    batchItemsOutputCursor.close();
		    }
		    batchItemsCursor.close();
		    batchItemsSelect.close();
        }
        finally {
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(batchItemsOutputCursor);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(batchItemsInputCursor);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(batchItemsOutputSelect);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(batchItemsInputSelect);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(batchItemsCursor);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(batchItemsSelect);
        	PostgreSQLUtils.getInstance(getParameterStore()).closeFinally(conn);
        }
        return retval;
	}
}
