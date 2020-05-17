package io.outofprintmagazine.corpus.storage.postgresql;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

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
import io.outofprintmagazine.corpus.storage.CorpusStorage;
import io.outofprintmagazine.corpus.storage.s3.S3Corpora;


public abstract class PostgreSQLCorpora implements CorpusStorage {

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(PostgreSQLCorpora.class);
	
	protected Logger getLogger() {
		return logger;
	}
	
	private CorpusStorage scratchStorage = null; //new S3Corpora();
	private ObjectMapper mapper = new ObjectMapper();
	

	protected ObjectMapper getMapper() {
		return mapper;
	}
	
	protected CorpusStorage getScratchStorage() {
		return scratchStorage;
	}
	
	//Path=//
	private Properties properties = new Properties();
	
	public Properties getProperties() {
		return properties;
	}
	
	public PostgreSQLCorpora() {
		super();
	}
	
	public PostgreSQLCorpora(Properties properties) {
		this();
		properties.putAll(properties);
		//scratchStorage = new S3Corpora(properties);
	}

	@Override
	public ObjectNode listCorpora() throws Exception {
		ObjectNode json = getMapper().createObjectNode();
		ArrayNode corporaNode = json.putArray("Corpora");
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet cursor = null;
        try {
		    conn = PostgreSQLUtils.getInstance().getClient();
		    String sql = "SELECT schema_name FROM information_schema.schemata where schema_name not in ('pg_catalog', 'information_schema', 'public')"; 
		    pstmt = conn.prepareStatement(sql);
		    cursor = pstmt.executeQuery();
		    while(cursor.next()) {
		    	corporaNode.add(cursor.getString(1));
		    }
        }
        finally {
        	PostgreSQLUtils.getInstance().closeFinally(cursor);
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
		return json;
	}
	
	@Override
	public void createCorpus(String corpus) throws Exception {
		Connection conn = null;
		Statement stmt = null;
        try {
		    conn = PostgreSQLUtils.getInstance().getClient();
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
		    
		    stmt.close();
        }
        finally {
        	PostgreSQLUtils.getInstance().closeFinally(stmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
	}


	public void createStagingBatch(String corpus, String stagingBatchName) throws Exception {
		createCorpus(corpus);
		Connection conn = null;
		PreparedStatement pstmt = null;
        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "insert into staging_batches(corpus_batch_id, name) values (?, ?) ON CONFLICT DO NOTHING";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, stagingBatchName);
		    pstmt.setString(2, stagingBatchName);
		    pstmt.executeUpdate();
		    pstmt.close();
        }
        finally {
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
	}
	

	public void storeStagingBatchObject(String corpus, String stagingBatchName, CorpusBatch batch)
			throws Exception {
		createStagingBatch(corpus, stagingBatchName);
		Connection conn = null;
		PreparedStatement batchItemsInsert = null;
		PreparedStatement batchItemsInputInsert = null;
		PreparedStatement batchItemsOutputInsert = null;
        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
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
				    	batchItemsOutputInsert.setInt(5, inputId);
				    	batchItemsOutputInsert.setString(6, propertyName);
				    	batchItemsOutputInsert.setString(7, propertyValue);
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
        	PostgreSQLUtils.getInstance().closeFinally(batchItemsOutputInsert);
        	PostgreSQLUtils.getInstance().closeFinally(batchItemsInputInsert);
        	PostgreSQLUtils.getInstance().closeFinally(batchItemsInsert);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
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
		    conn = PostgreSQLUtils.getInstance().getClient();
		    String sql = "SELECT corpus_batch_id from staging_batches order by corpus_batch_id"; 
		    pstmt = conn.prepareStatement(sql);
		    cursor = pstmt.executeQuery();
		    while(cursor.next()) {
		    	corporaNode.add(cursor.getString(1));
		    }
        }
        finally {
        	PostgreSQLUtils.getInstance().closeFinally(cursor);
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
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
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
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
        	PostgreSQLUtils.getInstance().closeFinally(batchItemsOutputCursor);
        	PostgreSQLUtils.getInstance().closeFinally(batchItemsInputCursor);
        	PostgreSQLUtils.getInstance().closeFinally(batchItemsOutputSelect);
        	PostgreSQLUtils.getInstance().closeFinally(batchItemsInputSelect);
        	PostgreSQLUtils.getInstance().closeFinally(batchItemsCursor);
        	PostgreSQLUtils.getInstance().closeFinally(batchItemsSelect);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
        return retval;
	}


	@Override
	public ObjectNode storeCoreNLP(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "insert into core_nlp("
    		+ "corpus_batch_id"
    		+ ", document_id"
    		+ ", data"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, stagingBatchName);
		    pstmt.setString(2, in.get("esnlc_DocIDAnnotation").asText(scratchFileName));
	    	PGobject json = new PGobject();
	    	json.setType("json");
	    	json.setValue(getMapper().writeValueAsString(in));
	    	pstmt.setObject(5, json);
	    	pstmt.executeUpdate();
	    	pstmt.close();
        }
        finally {
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
        return getScratchStorage().storeCoreNLP(corpus, stagingBatchName, scratchFileName, properties, in);
	}

	@Override
	public ObjectNode getCoreNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return getScratchStorage().getCoreNLP(corpus, stagingBatchName, scratchFileName);
	}

	@Override
	public ObjectNode storeAsciiText(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, String in) throws Exception {
		Connection conn = null;
		PreparedStatement textInsert = null;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "insert into plain_text("
    		+ "corpus_batch_id"
    		+ ", document_id"
    		+ ", data"
    		+ ", data_tokens"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ", ?"
    		+ ", to_tsvector(?)"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    textInsert = conn.prepareStatement(sql);
		    textInsert.setString(1, stagingBatchName);
		    //TODO - use 		    pstmt.setString(2, in.get("esnlc_DocIDAnnotation").asText(scratchFileName));
		    textInsert.setString(2, scratchFileName);
		    textInsert.setString(3, in);
		    textInsert.setString(4, in);
		    textInsert.executeUpdate();
		    textInsert.close();
        }
        finally {
        	try {
        		if (textInsert != null) {
        			textInsert.close();
        			textInsert = null;
        		}
        	}
        	catch (Exception e) {
        		getLogger().error(e);
        		textInsert = null;
        	}
        	try {
        		if (conn != null) {
        			conn.close();
        			conn = null;
        		}
        	}
        	catch (Exception e) {
        		getLogger().error(e);
        		conn = null;
        	}
        }
        return getScratchStorage().storeAsciiText(corpus, stagingBatchName, scratchFileName, properties, in);
	}

	@Override
	public String getAsciiText(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return getScratchStorage().getAsciiText(corpus, stagingBatchName, scratchFileName);		
	}


	@Override
	public ObjectNode storePipelineInfo(String corpus, String stagingBatchName, String scratchFileName,
			ObjectNode properties, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "insert into pipeline_info("
    		+ "corpus_batch_id"
    		+ ", document_id"
    		+ ", data"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, stagingBatchName);
		    //TODO pstmt.setString(2, in.get("esnlc_DocIDAnnotation").asText(scratchFileName));
		    pstmt.setString(2, scratchFileName);
	    	PGobject json = new PGobject();
	    	json.setType("json");
	    	json.setValue(getMapper().writeValueAsString(in));
	    	pstmt.setObject(5, json);
	    	pstmt.executeUpdate();
	    	pstmt.close();
        }
        finally {
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
        return getScratchStorage().storePipelineInfo(corpus, stagingBatchName, scratchFileName, properties, in);
	}

	@Override
	public ObjectNode getPipelineInfo(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return getScratchStorage().getPipelineInfo(corpus, stagingBatchName, scratchFileName);
	}
	

	@Override
	public ObjectNode storeOOPNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode properties, ObjectNode in) throws Exception {
		storeOOPNLP_Document(corpus, stagingBatchName, scratchFileName, properties, in);
		storeOOPNLP_DocumentAggregateScores(corpus, stagingBatchName, scratchFileName, properties, in);
		storeOOPNLP_DocumentScores(corpus, stagingBatchName, scratchFileName, properties, in);
		storeOOPNLP_SentenceScores(corpus, stagingBatchName, scratchFileName, properties, in);
		storeOOPNLP_TokenScores(corpus, stagingBatchName, scratchFileName, properties, in);
		return getScratchStorage().storeOOPNLP(corpus, stagingBatchName, scratchFileName, properties, in);
	}
	
	public void storeOOPNLP_Document(String corpus, String stagingBatchName, String docId, ObjectNode properties, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "insert into oop_nlp("
    		+ "corpus_batch_id"
    		+ ", document_id"
    		+ ", data"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, stagingBatchName);
		    pstmt.setString(2, docId);
	    	PGobject json = new PGobject();
	    	json.setType("json");
	    	json.setValue(getMapper().writeValueAsString(in));
	    	pstmt.setObject(5, json);
	    	pstmt.executeUpdate();
	    	pstmt.close();
        }

        finally {
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
        
	}

	public void storeOOPNLP_DocumentAggregateScores(String corpus, String stagingBatchName, String docId, ObjectNode properties, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;
		PreparedStatement aggregateScoreInsert = null;
		PreparedStatement aggregateSubScoreInsert = null;

        try {
        	conn = PostgreSQLUtils.getInstance().getClient(corpus);
			String sql = "insert into document_aggregate_scores"
					+ " (document_id, score, score_raw, score_normalized, score_count, score_min, score_max, score_mean, score_median, score_stddev)"
					+ " values (?,?,?,?,?,?,?,?,?,?)";
			aggregateScoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_aggregate_subscores"
					+ " (document_id, score, subscore, subscore_raw, subscore_normalized, subscore_count, subscore_rank, subscore_percentage, subscore_percentile)"
					+ " values (?,?,?,?,?,?,?,?,?)";
			aggregateSubScoreInsert = conn.prepareStatement(sql);
			Iterator<String> fieldsIter = in.fieldNames();
			while (fieldsIter.hasNext()) {
				String score = fieldsIter.next();
				if (score.endsWith("Aggregate") && !in.get(score).isNull()) {
					ObjectNode aggregateScore = (ObjectNode) in.get(score);
					String scoreName = score.substring(0, score.length()-"Aggregate".length());
					aggregateScoreInsert.setString(1, docId);
					aggregateScoreInsert.setString(2, scoreName);
					aggregateScoreInsert.setBigDecimal(3, aggregateScore.get("scoreStats").get("score").get("raw").decimalValue());
					aggregateScoreInsert.setBigDecimal(4, aggregateScore.get("scoreStats").get("score").get("normalized").decimalValue());
					aggregateScoreInsert.setBigDecimal(5, aggregateScore.get("scoreStats").get("score").get("count").decimalValue());
					aggregateScoreInsert.setBigDecimal(6, aggregateScore.get("scoreStats").get("stats").get("min").decimalValue());
					aggregateScoreInsert.setBigDecimal(7, aggregateScore.get("scoreStats").get("stats").get("max").decimalValue());
					aggregateScoreInsert.setBigDecimal(8, aggregateScore.get("scoreStats").get("stats").get("mean").decimalValue());
					aggregateScoreInsert.setBigDecimal(9, aggregateScore.get("scoreStats").get("stats").get("median").decimalValue());
					aggregateScoreInsert.setBigDecimal(10, aggregateScore.get("scoreStats").get("stats").get("stddev").decimalValue());
					aggregateScoreInsert.executeUpdate();
					for (JsonNode subScore : ((ArrayNode)aggregateScore.get("aggregatedScores"))) {
						aggregateSubScoreInsert.setString(1, docId);
						aggregateSubScoreInsert.setString(2, scoreName);
						aggregateSubScoreInsert.setString(3, subScore.get("name").asText());
						aggregateSubScoreInsert.setBigDecimal(4, subScore.get("score").get("raw").decimalValue());
						aggregateSubScoreInsert.setBigDecimal(5, subScore.get("score").get("normalized").decimalValue());
						aggregateSubScoreInsert.setBigDecimal(6, subScore.get("score").get("count").decimalValue());
						aggregateSubScoreInsert.setBigDecimal(7, subScore.get("aggregateScore").get("rank").decimalValue());
						aggregateSubScoreInsert.setBigDecimal(8, subScore.get("aggregateScore").get("percentage").decimalValue());
						aggregateSubScoreInsert.setBigDecimal(9, subScore.get("aggregateScore").get("percentile").decimalValue());
						aggregateSubScoreInsert.executeUpdate();
					}
				}
			}
        }


	    finally {
	    	PostgreSQLUtils.getInstance().closeFinally(pstmt);
	    	PostgreSQLUtils.getInstance().closeFinally(aggregateScoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(aggregateSubScoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(conn);
	    }
	}

	public void storeOOPNLP_DocumentScores(String corpus, String stagingBatchName, String docId, ObjectNode properties, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement scoreInsert = null;
		PreparedStatement subscoreInsert = null;
		List<String> metadata = Arrays.asList(
		     		"DocIDAnnotation",
		     		"DocTitleAnnotation",
		     		"AuthorAnnotation",
		     		"DocDateAnnotation",
		     		"DocTypeAnnotation",
		     		"DocSourceTypeAnnotation"
		     	);
        try {
        	conn = PostgreSQLUtils.getInstance().getClient(corpus);
			String sql = "insert into document_scores"
					+ " (document_id, score, score_raw, score_normalized)"
					+ " values (?,?,?,?)";
			scoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_subscores"
					+ " (document_id, score, subscore, subscore_raw, subscore_normalized)"
					+ " values (?,?,?,?,?)";
			subscoreInsert = conn.prepareStatement(sql);
			BigDecimal tokenCount = new BigDecimal(in.get("OOPDocumentLengthAnnotation").get("wordCount").asInt());
			Iterator<String> fieldsIter = in.fieldNames();
			while (fieldsIter.hasNext()) {
				String score = fieldsIter.next();
				if (!score.endsWith("Aggregate") && !metadata.contains(score)) {
					JsonNode scoreObject = in.get(score);
					String scoreName = score;
					if (scoreObject.isObject()) {
						Iterator<String> subscoreIter = scoreObject.fieldNames();
						while (subscoreIter.hasNext()) {
							String subscoreName = subscoreIter.next();
							subscoreInsert.setString(1, docId);
							subscoreInsert.setString(2, scoreName);
							subscoreInsert.setString(3, subscoreName);
							subscoreInsert.setBigDecimal(4, scoreObject.get(subscoreName).decimalValue());
							subscoreInsert.setBigDecimal(5, scoreObject.get(subscoreName).decimalValue().divide(tokenCount));
							subscoreInsert.executeUpdate();
						}
					}
					else {
						scoreInsert.setString(1, docId);
						scoreInsert.setString(2, scoreName);
						scoreInsert.setBigDecimal(3, scoreObject.decimalValue());
						scoreInsert.setBigDecimal(4, scoreObject.decimalValue().divide(tokenCount));
						scoreInsert.executeUpdate();						
					}
				}
			}
        }

	    finally {
	    	PostgreSQLUtils.getInstance().closeFinally(scoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(subscoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(conn);
	    }
	}

	public void storeOOPNLP_SentenceScores(String corpus, String stagingBatchName, String docId, ObjectNode properties, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement sentenceInsert = null;
		PreparedStatement scoreInsert = null;
		PreparedStatement subscoreInsert = null;
		List<String> metadata = Arrays.asList(
		     		"SentenceIndexAnnotation",
		     		"text",
		     		"tokens"
		     	);
        try {
        	conn = PostgreSQLUtils.getInstance().getClient(corpus);
			String sql = "insert into document_sentences"
					+ " (document_id, sentence_id, sentence_text)"
					+ " values (?,?,?)";
			sentenceInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_scores"
					+ " (document_id, sentence_id, score, score_raw, score_normalized)"
					+ " values (?,?,?,?)";
			scoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_subscores"
					+ " (document_id, sentence_id, score, subscore, subscore_raw, subscore_normalized)"
					+ " values (?,?,?,?,?)";
			subscoreInsert = conn.prepareStatement(sql);
			BigDecimal tokenCount = new BigDecimal(in.get("OOPDocumentLengthAnnotation").get("wordCount").asInt());
			for (JsonNode sentence : in.get("sentences")) {
				sentenceInsert.setString(1, docId);
				sentenceInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
				sentenceInsert.setString(3, sentence.get("text").asText());
				sentenceInsert.executeUpdate();
				Iterator<String> fieldsIter = in.fieldNames();
				while (fieldsIter.hasNext()) {
					String score = fieldsIter.next();
					if (!metadata.contains(score)) {
						JsonNode scoreObject = in.get(score);
						String scoreName = score;
						if (scoreObject.isObject()) {
							Iterator<String> subscoreIter = scoreObject.fieldNames();
							while (subscoreIter.hasNext()) {
								String subscoreName = subscoreIter.next();
								subscoreInsert.setString(1, docId);
								subscoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
								subscoreInsert.setString(3, scoreName);
								subscoreInsert.setString(4, subscoreName);
								subscoreInsert.setBigDecimal(5, scoreObject.get(subscoreName).decimalValue());
								subscoreInsert.setBigDecimal(6, scoreObject.get(subscoreName).decimalValue().divide(tokenCount));
								subscoreInsert.executeUpdate();
							}
						}
						else {
							scoreInsert.setString(1, docId);
							scoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
							scoreInsert.setString(3, scoreName);
							scoreInsert.setBigDecimal(4, scoreObject.decimalValue());
							scoreInsert.setBigDecimal(5, scoreObject.decimalValue().divide(tokenCount));
							scoreInsert.executeUpdate();						
						}
					}
				}
			}
        }

	    finally {
	    	PostgreSQLUtils.getInstance().closeFinally(sentenceInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(scoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(subscoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(conn);
	    }
	}
	
	public void storeOOPNLP_TokenScores(String corpus, String stagingBatchName, String docId, ObjectNode properties, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement tokenInsert = null;
		PreparedStatement scoreInsert = null;
		PreparedStatement subscoreInsert = null;
		PreparedStatement syllableScoreInsert = null;
		PreparedStatement syllableSubscoreInsert = null;
		List<String> metadata = Arrays.asList(
		     		"tokenIndex",
		     		"TokensAnnotation"
		     	);
        try {
        	conn = PostgreSQLUtils.getInstance().getClient(corpus);
			String sql = "insert into document_sentence_tokens"
					+ " (document_id, sentence_id, token_id"
					+ ", word"
					+ ", originalText" 
					+ ", lemma"
					+ ", characterOffsetBegin" 
					+ ", characterOffsetEnd" 
					+ ", pos" 
					+ ", ner" 
					+ ", before" 
					+ ", after)"
					+ " values (?,?,?"
					+ ",?,?,?,?,?,?,?,?,?)";
			tokenInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_token_scores"
					+ " (document_id, sentence_id, token_id, score, score_raw, score_normalized)"
					+ " values (?,?,?,?,?,?)";
			scoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_token_subscores"
					+ " (document_id, sentence_id, token_id, score, subscore, subscore_raw, subscore_normalized)"
					+ " values (?,?,?,?,?,?,?)";
			subscoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_token_syllable_scores"
					+ " (document_id, sentence_id, token_id, score, score_raw, score_normalized, syllable_id)"
					+ " values (?,?,?,?,?,?,?)";
			syllableScoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_token_syllable_subscores"
					+ " (document_id, sentence_id, token_id, score, subscore, subscore_raw, subscore_normalized, syllable_id)"
					+ " values (?,?,?,?,?,?,?,?)";
			syllableSubscoreInsert = conn.prepareStatement(sql);
			BigDecimal tokenCount = new BigDecimal(in.get("OOPDocumentLengthAnnotation").get("wordCount").asInt());
			for (JsonNode sentence : in.get("sentences")) {
				for (JsonNode token : sentence.get("tokens")) {
					tokenInsert.setString(1, docId);
					tokenInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
					tokenInsert.setInt(3, token.get("tokenIndex").asInt());
					tokenInsert.setString(4,  token.get("TokensAnnotation").get("word").asText());
					tokenInsert.setString(5,  token.get("TokensAnnotation").get("originalText").asText());
					tokenInsert.setString(6,  token.get("TokensAnnotation").get("lemma").asText());
					tokenInsert.setInt(7,  token.get("TokensAnnotation").get("characterOffsetBegin").asInt());
					tokenInsert.setInt(8,  token.get("TokensAnnotation").get("characterOffsetEnd").asInt());
					tokenInsert.setString(9,  token.get("TokensAnnotation").get("pos").asText());
					tokenInsert.setString(10,  token.get("TokensAnnotation").get("ner").asText());
					tokenInsert.setString(11,  token.get("TokensAnnotation").get("before").asText());
					tokenInsert.setString(12,  token.get("TokensAnnotation").get("after").asText());
					tokenInsert.executeUpdate();
					Iterator<String> fieldsIter = in.fieldNames();
					while (fieldsIter.hasNext()) {
						String score = fieldsIter.next();
						if (!metadata.contains(score)) {
							JsonNode scoreObject = in.get(score);
							String scoreName = score;
							if (scoreObject.isObject()) {
								Iterator<String> subscoreIter = scoreObject.fieldNames();
								while (subscoreIter.hasNext()) {
									String subscoreName = subscoreIter.next();
									subscoreInsert.setString(1, docId);
									subscoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
									subscoreInsert.setInt(3, token.get("tokenIndex").asInt());
									subscoreInsert.setString(4, scoreName);
									subscoreInsert.setString(5, subscoreName);
									subscoreInsert.setBigDecimal(6, scoreObject.get(subscoreName).decimalValue());
									subscoreInsert.setBigDecimal(7, scoreObject.get(subscoreName).decimalValue().divide(tokenCount));
									subscoreInsert.executeUpdate();
									syllableSubscoreInsert.setString(1, docId);
									syllableSubscoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
									syllableSubscoreInsert.setInt(3, token.get("tokenIndex").asInt());
									syllableSubscoreInsert.setString(4, scoreName);
									syllableSubscoreInsert.setString(5, subscoreName);
									syllableSubscoreInsert.setBigDecimal(6, scoreObject.get(subscoreName).decimalValue());
									syllableSubscoreInsert.setBigDecimal(7, scoreObject.get(subscoreName).decimalValue().divide(tokenCount));
									if (!scoreName.equals("OOPSyllablesAnnotation")) {
										for (int i=0;i<token.get("OOPSyllablesAnnotation").asInt();i++) {
											syllableSubscoreInsert.setInt(8,  i);
											syllableSubscoreInsert.executeUpdate();
										}
									}
								}
							}
							else {
								scoreInsert.setString(1, docId);
								scoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
								scoreInsert.setInt(3, token.get("tokenIndex").asInt());
								scoreInsert.setString(4, scoreName);
								scoreInsert.setBigDecimal(5, scoreObject.decimalValue());
								scoreInsert.setBigDecimal(6, scoreObject.decimalValue().divide(tokenCount));
								scoreInsert.executeUpdate();
								syllableSubscoreInsert.setString(1, docId);
								syllableSubscoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
								syllableSubscoreInsert.setInt(3, token.get("tokenIndex").asInt());
								syllableSubscoreInsert.setString(4, scoreName);
								syllableSubscoreInsert.setBigDecimal(5, scoreObject.decimalValue());
								syllableSubscoreInsert.setBigDecimal(6, scoreObject.decimalValue().divide(tokenCount));
								if (!scoreName.equals("OOPSyllablesAnnotation")) {
									for (int i=0;i<token.get("OOPSyllablesAnnotation").asInt();i++) {
										syllableSubscoreInsert.setInt(7,  i);
										syllableSubscoreInsert.executeUpdate();
									}
								}
							}
						}
					}
				}
	        }
        }

	    finally {
	    	PostgreSQLUtils.getInstance().closeFinally(tokenInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(scoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(subscoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(syllableScoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(syllableSubscoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(conn);
	    }
	}
	
	@Override
	public ObjectNode getOOPNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		return getScratchStorage().getOOPNLP(corpus, stagingBatchName, scratchFileName);
	}
	

	

	@Override
	public ObjectNode storeScratchFileString(String corpus, String scratchFileName, ObjectNode properties, String in) throws Exception {
		return getScratchStorage().storeScratchFileString(corpus, scratchFileName, properties, in);
	}

	@Override
	public ObjectNode storeScratchFileStream(String corpus, String scratchFileName,	ObjectNode properties, InputStream in) throws Exception {
		return getScratchStorage().storeScratchFileStream(corpus, scratchFileName, properties, in);
	}

	@Override
	public InputStream getScratchFileStream(String corpus, String scratchFileName)
			throws Exception {
		return getScratchStorage().getScratchFileStream(corpus, scratchFileName);
	}

	@Override
	public String getScratchFileString(String corpus, String scratchFileName)
			throws Exception {
		return getScratchStorage().getScratchFileString(corpus, scratchFileName);
	}

	@Override
	public InputStream getScratchFilePropertiesStream(String corpus, String scratchFileName)
			throws Exception {
		return getScratchStorage().getScratchFilePropertiesStream(corpus, scratchFileName);
	}

	@Override
	public String getFileNameFromPath(String scratchFilePath) {
		return getScratchStorage().getFileNameFromPath(scratchFilePath);
	}

	@Override
	public String trimFileExtension(String scratchFileName) {
		return getScratchStorage().trimFileExtension(scratchFileName);
	}

	@Override
	public String getScratchFilePath(String stagingBatchName, String stagingBatchStepName, String scratchFileName)
			throws Exception {
		return getScratchStorage().getScratchFilePath(stagingBatchName, stagingBatchStepName, scratchFileName);
	}

	@Override
	public ObjectNode getScratchFileProperties(String corpus, String scratchFilePath) throws Exception {
		return getScratchStorage().getScratchFileProperties(corpus, scratchFilePath);
	}
	
}
