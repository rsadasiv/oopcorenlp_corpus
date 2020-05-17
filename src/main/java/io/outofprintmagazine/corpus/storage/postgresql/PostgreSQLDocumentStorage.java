package io.outofprintmagazine.corpus.storage.postgresql;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.storage.DocumentStorage;
import io.outofprintmagazine.nlp.pipeline.PhraseAnnotation;


public class PostgreSQLDocumentStorage implements DocumentStorage {

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(PostgreSQLDocumentStorage.class);
	
	protected Logger getLogger() {
		return logger;
	}
	
	private ObjectMapper mapper = new ObjectMapper();
	

	protected ObjectMapper getMapper() {
		return mapper;
	}
	
	//Path=//
	private Properties properties = new Properties();
	
	public Properties getProperties() {
		return properties;
	}
	
	public PostgreSQLDocumentStorage() {
		super();
	}
	
	public PostgreSQLDocumentStorage(Properties properties) {
		this();
		properties.putAll(properties);
	}



	@Override
	public void storeCoreNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "insert into core_nlp("
    		+ "document_id"
    		+ ", data"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, scratchFileName);
	    	PGobject json = new PGobject();
	    	json.setType("json");
	    	json.setValue(getMapper().writeValueAsString(in));
	    	pstmt.setObject(2, json);
	    	pstmt.executeUpdate();
	    	pstmt.close();
        }
        finally {
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }

	}

	@Override
	public ObjectNode getCoreNLP(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet cursor = null;
		ObjectNode retval = null;
        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "select data from core_nlp where "
    		+ "document_id = ? ";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, scratchFileName);
		    cursor = pstmt.executeQuery();
		    if (cursor.next()) {
		    	retval = (ObjectNode) getMapper().readTree(cursor.getString("data"));
		    }
		    return retval;
        }

        finally {
        	PostgreSQLUtils.getInstance().closeFinally(cursor);
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        } 
	}

	@Override
	public void storeAsciiText(String corpus, String stagingBatchName, String scratchFileName, String in) throws Exception {
		Connection conn = null;
		PreparedStatement textInsert = null;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "insert into documents("
    		+ "document_id"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ")"
    		+ "ON CONFLICT DO NOTHING";
		    textInsert = conn.prepareStatement(sql);
		    textInsert.setString(1, scratchFileName);
		    textInsert.executeUpdate();
		    textInsert.close();
		    
		    sql = "insert into document_texts("
    		+ "document_id"
    		+ ", data"
    		+ ", data_tokens"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ", to_tsvector(?)"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    textInsert = conn.prepareStatement(sql);
		    //textInsert.setString(1, stagingBatchName);
		    //TODO - use 		    pstmt.setString(2, in.get("esnlc_DocIDAnnotation").asText(scratchFileName));
		    textInsert.setString(1, scratchFileName);
		    textInsert.setString(2, in);
		    textInsert.setString(3, in);
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

	}

	@Override
	public String getAsciiText(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet cursor = null;
		String retval = null;
        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "select data from document_texts where "
    		+ "document_id = ? ";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, scratchFileName);
		    cursor = pstmt.executeQuery();
		    if (cursor.next()) {
		    	retval = cursor.getString("data");
		    }
		    return retval;
        }

        finally {
        	PostgreSQLUtils.getInstance().closeFinally(cursor);
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }	
	}


	@Override
	public void storePipelineInfo(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "insert into pipeline_info("
    		+ "document_id"
    		+ ", data"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, scratchFileName);
	    	PGobject json = new PGobject();
	    	json.setType("json");
	    	json.setValue(getMapper().writeValueAsString(in));
	    	pstmt.setObject(2, json);
	    	pstmt.executeUpdate();
	    	pstmt.close();
	    	
		    sql = "insert into scores("
    		+ "score"
    		+ ", description"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    pstmt = conn.prepareStatement(sql);
		    ArrayNode annotations = (ArrayNode) in.get("annotations");
		    for (JsonNode annotation : annotations) {
		    	Iterator<String> fieldNames = annotation.fieldNames();
		    	while (fieldNames.hasNext()) {
		    		String fieldName = fieldNames.next();
		    		pstmt.setString(1, fieldName);
		    		pstmt.setString(2, annotation.get(fieldName).asText(fieldName));
		    		pstmt.executeUpdate();
		    		pstmt.setString(1, fieldName+"Aggregate");
		    		pstmt.setString(2, "io.outofprintmagazine.util.CorpusDocumentAggregateScore");
		    		pstmt.executeUpdate();
		    		if (fieldName.equals("OOPQuotesAnnotation")) {
			    		pstmt.setString(1, fieldName+"List");
			    		pstmt.setString(2, "speakers and quotes");
			    		pstmt.executeUpdate();
		    		}
		    		if (fieldName.equals("PerfecttenseAnnotation")) {
			    		pstmt.setString(1, "PerfecttenseGlossAnnotation");
			    		pstmt.setString(2, annotation.get(fieldName).asText(fieldName));
			    		pstmt.executeUpdate();
		    		}
		    		
		    	}
		    }
		    pstmt.close();
	    	
        }
        finally {
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
	}

	@Override
	public ObjectNode getPipelineInfo(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet cursor = null;
		ObjectNode retval = null;
        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "select data from pipeline_info where "
    		+ "document_id = ? ";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, scratchFileName);
		    cursor = pstmt.executeQuery();
		    if (cursor.next()) {
		    	retval = (ObjectNode) getMapper().readTree(cursor.getString("data"));
		    }
		    return retval;
        }

        finally {
        	PostgreSQLUtils.getInstance().closeFinally(cursor);
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
	}
	

	@Override
	public void storeOOPNLP(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception {
		storeOOPNLP_Document(corpus, stagingBatchName, scratchFileName, in);
		storeOOPNLP_DocumentScores(corpus, stagingBatchName, scratchFileName, in);
		storeOOPNLP_SentenceScores(corpus, stagingBatchName, scratchFileName, in);
		storeOOPNLP_TokenScores(corpus, stagingBatchName, scratchFileName, in);

	}
	
	public void storeOOPNLP_Document(String corpus, String stagingBatchName, String docId, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "insert into oop_nlp("
    		+ "document_id"
    		+ ", data"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, docId);
	    	PGobject json = new PGobject();
	    	json.setType("json");
	    	json.setValue(getMapper().writeValueAsString(in));
	    	pstmt.setObject(2, json);
	    	pstmt.executeUpdate();
	    	pstmt.close();
	    	sql = "insert into document_properties("
	    			+ "document_id" + 
	    			", property_name" + 
	    			", property_value"
	    			+ ")"
	    			+ "values ("
	    			+ "?"
	    			+ ",?"
	    			+ ",?"
	    			+ ")"
	    			+ "ON CONFLICT DO NOTHING";
	    	pstmt = conn.prepareStatement(sql);
	    	pstmt.setString(1, docId);
			Iterator<String> fieldsIter = in.get("metadata").fieldNames();
			while (fieldsIter.hasNext()) {
				String score = fieldsIter.next();
				pstmt.setString(2,  score);
				pstmt.setString(3, in.get("metadata").get(score).asText());
				pstmt.executeUpdate();
			}
        }

        finally {
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        } 
	}

	public void storeOOPNLP_DocumentScores(String corpus, String stagingBatchName, String docId, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement scoreInsert = null;
		PreparedStatement subscoreInsert = null;

        try {
        	conn = PostgreSQLUtils.getInstance().getClient(corpus);
			String sql = "insert into document_scores"
					+ " (document_id, score, score_raw, score_normalized)"
					+ " values (?,?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			scoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_subscores"
					+ " (document_id, score, subscore, subscore_raw, subscore_normalized)"
					+ " values (?,?,?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			subscoreInsert = conn.prepareStatement(sql);
			BigDecimal tokenCount = new BigDecimal(in.get("OOPWordCountAnnotation").asInt());
			Iterator<Entry<String, JsonNode>> fieldsIter = in.fields();
			while (fieldsIter.hasNext()) {
				Entry<String, JsonNode> field = fieldsIter.next();
				String scoreName = field.getKey();
				JsonNode scoreObject = field.getValue();
				if (
						!scoreName.equals("sentences") 
						&& !scoreName.equals("metadata")
						&& !scoreName.equals("corefs")
						&& !scoreName.equals("quotes")
				) {
					if (scoreObject.isArray() ) {
						List<PhraseAnnotation> typedScore = getMapper().convertValue(
								scoreObject, 
								getMapper().getTypeFactory().constructCollectionType(List.class, PhraseAnnotation.class)
						);
						for (PhraseAnnotation phraseAnnotation : typedScore) {
							subscoreInsert.setString(1, docId);
							subscoreInsert.setString(2, scoreName);
							subscoreInsert.setString(3, phraseAnnotation.getName().substring(0, phraseAnnotation.getName().length()>500?500:phraseAnnotation.getName().length()));
							subscoreInsert.setBigDecimal(4, phraseAnnotation.getValue());
							subscoreInsert.setBigDecimal(5, phraseAnnotation.getValue().divide(tokenCount, 10, RoundingMode.HALF_DOWN));
							subscoreInsert.executeUpdate();
						}
					}
					else if (scoreObject.isObject()) {
						//TODO
						//Contextual scores?
						if (!scoreName.equals("OOPActorsAnnotation") && !scoreName.equals("OOPSettingsAnnotation")) {
							Map<String, BigDecimal> typedScore = getMapper().convertValue(
									scoreObject, 
									new TypeReference<Map<String, BigDecimal>>(){}
							);
							for (Entry<String, BigDecimal> subscore : typedScore.entrySet()) {
								String subscoreName = subscore.getKey();
								subscoreInsert.setString(1, docId);
								subscoreInsert.setString(2, scoreName);
								subscoreInsert.setString(3, subscoreName);
								subscoreInsert.setBigDecimal(4, typedScore.get(subscoreName));
								subscoreInsert.setBigDecimal(5, typedScore.get(subscoreName).divide(tokenCount, 10, RoundingMode.HALF_DOWN));
								subscoreInsert.executeUpdate();
							}
						}
					}
					else if (scoreObject.isBigDecimal()) {
						BigDecimal typedScore = getMapper().convertValue(
								scoreObject,
								BigDecimal.class
						);
						scoreInsert.setString(1, docId);
						scoreInsert.setString(2, scoreName);
						scoreInsert.setBigDecimal(3, typedScore);
						scoreInsert.setBigDecimal(4, typedScore.divide(tokenCount, 10, RoundingMode.HALF_DOWN));
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

	public void storeOOPNLP_SentenceScores(String corpus, String stagingBatchName, String docId, ObjectNode in) throws Exception {
		Connection conn = null;
		PreparedStatement sentenceInsert = null;
		PreparedStatement scoreInsert = null;
		PreparedStatement subscoreInsert = null;
		boolean autoCommit = true;
		
		List<String> metadata = Arrays.asList(
		     		"SentenceIndexAnnotation",
		     		"text",
		     		"tokens"
		     	);
        try {
        	conn = PostgreSQLUtils.getInstance().getClient(corpus);

    		autoCommit = conn.getAutoCommit();
    		conn.setAutoCommit(false);
			String sql = "insert into document_sentences"
					+ " (document_id, sentence_id, sentence_text)"
					+ " values (?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			sentenceInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_scores"
					+ " (document_id, sentence_id, score, score_raw, score_normalized)"
					+ " values (?,?,?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			scoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_subscores"
					+ " (document_id, sentence_id, score, subscore, subscore_raw, subscore_normalized)"
					+ " values (?,?,?,?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			subscoreInsert = conn.prepareStatement(sql);
			BigDecimal tokenCount = new BigDecimal(in.get("OOPWordCountAnnotation").asInt());
			for (JsonNode sentence : in.get("sentences")) {
				sentenceInsert.setString(1, docId);
				sentenceInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
				sentenceInsert.setString(3, sentence.get("text").asText());
				sentenceInsert.executeUpdate();

				Iterator<String> fieldsIter = sentence.fieldNames();
				while (fieldsIter.hasNext()) {
					String score = fieldsIter.next();
					if (!score.equals("tokens")) {
						if (!metadata.contains(score)) {
							String scoreName = score;
							JsonNode scoreObject = sentence.get(score);
							if (scoreObject.isArray() ) {
								List<PhraseAnnotation> typedScore = getMapper().convertValue(
										scoreObject, 
										getMapper().getTypeFactory().constructCollectionType(List.class, PhraseAnnotation.class)
								);
								for (PhraseAnnotation phraseAnnotation : typedScore) {
									subscoreInsert.setString(1, docId);
									subscoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
									subscoreInsert.setString(3, scoreName);
									subscoreInsert.setString(4, 
											phraseAnnotation.getName().substring(
													0, 
													phraseAnnotation.getName().length()>500?500:phraseAnnotation.getName().length()
											)
									);
									subscoreInsert.setBigDecimal(5, phraseAnnotation.getValue());
									subscoreInsert.setBigDecimal(6, phraseAnnotation.getValue().divide(tokenCount, 10, RoundingMode.HALF_DOWN));
									//subscoreInsert.executeUpdate();
									subscoreInsert.addBatch();
								}
							}
							else if (scoreObject.isObject()) {
								Map<String, BigDecimal> typedScore = getMapper().convertValue(
										scoreObject, 
										new TypeReference<Map<String, BigDecimal>>(){}
								);
								for (Entry<String, BigDecimal> subscore : typedScore.entrySet()) {
									String subscoreName = subscore.getKey();
									subscoreInsert.setString(1, docId);
									subscoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
									subscoreInsert.setString(3, scoreName);
									subscoreInsert.setString(4, subscoreName);
									subscoreInsert.setBigDecimal(5, typedScore.get(subscoreName));
									subscoreInsert.setBigDecimal(6, typedScore.get(subscoreName).divide(tokenCount, 10, RoundingMode.HALF_DOWN));
									//subscoreInsert.executeUpdate();
									subscoreInsert.addBatch();
								}
							}
							else if (scoreObject.isBigDecimal()) {
								BigDecimal typedScore = getMapper().convertValue(
										scoreObject,
										BigDecimal.class
								);
								scoreInsert.setString(1, docId);
								scoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
								scoreInsert.setString(3, scoreName);
								scoreInsert.setBigDecimal(4, typedScore);
								scoreInsert.setBigDecimal(5, typedScore.divide(tokenCount, 10, RoundingMode.HALF_DOWN));
								//scoreInsert.executeUpdate();
								scoreInsert.addBatch();
										
							}
						}
					}
				}
				subscoreInsert.executeBatch();
				scoreInsert.executeBatch();
			}
	    	conn.commit();
        }

	    finally {
	    	conn.setAutoCommit(autoCommit);
	    	PostgreSQLUtils.getInstance().closeFinally(sentenceInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(scoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(subscoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(conn);
	    }
	}
	
	public void storeOOPNLP_TokenScores(String corpus, String stagingBatchName, String docId, ObjectNode in) throws Exception {
		Connection conn = null;
		boolean autoCommit = true;
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
    		autoCommit = conn.getAutoCommit();
    		conn.setAutoCommit(false);
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
					+ ",?,?,?,?,?,?,?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			tokenInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_token_scores"
					+ " (document_id, sentence_id, token_id, score, score_raw, score_normalized)"
					+ " values (?,?,?,?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			scoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_token_subscores"
					+ " (document_id, sentence_id, token_id, score, subscore, subscore_raw, subscore_normalized)"
					+ " values (?,?,?,?,?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			subscoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_token_syllable_scores"
					+ " (document_id, sentence_id, token_id, score, score_raw, score_normalized, syllable_id)"
					+ " values (?,?,?,?,?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			syllableScoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_sentence_token_syllable_subscores"
					+ " (document_id, sentence_id, token_id, score, subscore, subscore_raw, subscore_normalized, syllable_id)"
					+ " values (?,?,?,?,?,?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			syllableSubscoreInsert = conn.prepareStatement(sql);
			BigDecimal tokenCount = new BigDecimal(in.get("OOPWordCountAnnotation").asInt());
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
					Iterator<String> fieldsIter = token.fieldNames();
					while (fieldsIter.hasNext()) {
						String score = fieldsIter.next();
						if (!metadata.contains(score)) {
							JsonNode scoreObject = token.get(score);
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
									subscoreInsert.setBigDecimal(7, scoreObject.get(subscoreName).decimalValue().divide(tokenCount, 10, RoundingMode.HALF_DOWN));
									//subscoreInsert.executeUpdate();
									subscoreInsert.addBatch();
									syllableSubscoreInsert.setString(1, docId);
									syllableSubscoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
									syllableSubscoreInsert.setInt(3, token.get("tokenIndex").asInt());
									syllableSubscoreInsert.setString(4, scoreName);
									syllableSubscoreInsert.setString(5, subscoreName);
									syllableSubscoreInsert.setBigDecimal(6, scoreObject.get(subscoreName).decimalValue());
									syllableSubscoreInsert.setBigDecimal(7, scoreObject.get(subscoreName).decimalValue().divide(tokenCount, 10, RoundingMode.HALF_DOWN));
									if (!scoreName.equals("OOPSyllablesAnnotation") && token.has("OOPSyllablesAnnotation")) {
										for (int i=0;i<token.get("OOPSyllablesAnnotation").asInt(1);i++) {
											syllableSubscoreInsert.setInt(8,  i);
											//syllableSubscoreInsert.executeUpdate();
											syllableSubscoreInsert.addBatch();
										}
									}
								}
							}
							else if (scoreObject.isBigDecimal()) { 
								scoreInsert.setString(1, docId);
								scoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
								scoreInsert.setInt(3, token.get("tokenIndex").asInt());
								scoreInsert.setString(4, scoreName);
								scoreInsert.setBigDecimal(5, scoreObject.decimalValue());
								scoreInsert.setBigDecimal(6, scoreObject.decimalValue().divide(tokenCount, 10, RoundingMode.HALF_DOWN));
								//scoreInsert.executeUpdate();
								scoreInsert.addBatch();
								syllableSubscoreInsert.setString(1, docId);
								syllableSubscoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
								syllableSubscoreInsert.setInt(3, token.get("tokenIndex").asInt());
								syllableSubscoreInsert.setString(4, scoreName);
								syllableSubscoreInsert.setBigDecimal(5, scoreObject.decimalValue());
								syllableSubscoreInsert.setBigDecimal(6, scoreObject.decimalValue().divide(tokenCount, 10, RoundingMode.HALF_DOWN));
								if (!scoreName.equals("OOPSyllablesAnnotation") && token.has("OOPSyllablesAnnotation")) {
									for (int i=0;i<token.get("OOPSyllablesAnnotation").asInt(1);i++) {
										syllableSubscoreInsert.setInt(7,  i);
										//syllableSubscoreInsert.executeUpdate();
										syllableSubscoreInsert.addBatch();
									}
								}
							}
							else if (scoreObject.isTextual()) {
								scoreInsert.setString(1, docId);
								scoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
								scoreInsert.setInt(3, token.get("tokenIndex").asInt());
								scoreInsert.setString(4, scoreName);
								scoreInsert.setBigDecimal(5, new BigDecimal(1));
								scoreInsert.setBigDecimal(6, new BigDecimal(1).divide(tokenCount, 10, RoundingMode.HALF_DOWN));
								//scoreInsert.executeUpdate();
								scoreInsert.addBatch();
								syllableSubscoreInsert.setString(1, docId);
								syllableSubscoreInsert.setInt(2, sentence.get("SentenceIndexAnnotation").asInt());
								syllableSubscoreInsert.setInt(3, token.get("tokenIndex").asInt());
								syllableSubscoreInsert.setString(4, scoreName);
								syllableSubscoreInsert.setBigDecimal(5, new BigDecimal(1));
								syllableSubscoreInsert.setBigDecimal(6, new BigDecimal(1).divide(tokenCount, 10, RoundingMode.HALF_DOWN));
								if (!scoreName.equals("OOPSyllablesAnnotation") && token.has("OOPSyllablesAnnotation")) {
									for (int i=0;i<token.get("OOPSyllablesAnnotation").asInt(1);i++) {
										syllableSubscoreInsert.setInt(7,  i);
										//syllableSubscoreInsert.executeUpdate();
										syllableSubscoreInsert.addBatch();
									}
								}
							}
						}
					}
					subscoreInsert.executeBatch();
					syllableSubscoreInsert.executeBatch();
					scoreInsert.executeBatch();
				}
	        }
			conn.commit();
        }

	    finally {
	    	conn.setAutoCommit(autoCommit);
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
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet cursor = null;
		ObjectNode retval = null;
        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "select data from oop_nlp where "
    		+ "document_id = ? ";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, scratchFileName);
		    cursor = pstmt.executeQuery();
		    if (cursor.next()) {
		    	retval = (ObjectNode) getMapper().readTree(cursor.getString("data"));
		    }
		    return retval;
        }

        finally {
        	PostgreSQLUtils.getInstance().closeFinally(cursor);
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        } 
	}


	@Override
	public ObjectNode getOOPAggregates(String corpus, String stagingBatchName, String scratchFileName) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet cursor = null;
		ObjectNode retval = null;
        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    String sql = "select data from oop_nlp_aggregates where "
    		+ "document_id = ? ";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, scratchFileName);
		    cursor = pstmt.executeQuery();
		    if (cursor.next()) {
		    	retval = (ObjectNode) getMapper().readTree(cursor.getString("data"));
		    }
		    return retval;
        }

        finally {
        	PostgreSQLUtils.getInstance().closeFinally(cursor);
        	PostgreSQLUtils.getInstance().closeFinally(pstmt);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        } 
	}

	@Override
	public void storeOOPAggregates(String corpus, String stagingBatchName, String docId, ObjectNode in) throws Exception {

		Connection conn = null;
		PreparedStatement pstmt = null;
		PreparedStatement aggregateScoreInsert = null;
		PreparedStatement aggregateListInsert = null;
		PreparedStatement aggregateSubScoreInsert = null;
		boolean autoCommit = true;
        try {
        	conn = PostgreSQLUtils.getInstance().getClient(corpus);
        	autoCommit = conn.getAutoCommit();
        	conn.setAutoCommit(false);
		    String sql = "insert into oop_nlp_aggregates("
    		+ "document_id"
    		+ ", data"
    		+ ") "
    		+ "values ("
    		+ "?"
    		+ ", ?"
    		+ ") "
    		+ "ON CONFLICT DO NOTHING";
		    pstmt = conn.prepareStatement(sql);
		    pstmt.setString(1, docId);
	    	PGobject json = new PGobject();
	    	json.setType("json");
	    	json.setValue(getMapper().writeValueAsString(in));
	    	pstmt.setObject(2, json);
	    	pstmt.executeUpdate();
	    	pstmt.close();
        	
			sql = "insert into document_aggregate_scores"
					+ " (document_id, score, score_raw, score_normalized, score_count, score_min, score_max, score_mean, score_median, score_stddev)"
					+ " values (?,?,?,?,?,?,?,?,?,?) "
		    		+ "ON CONFLICT DO NOTHING";
			aggregateScoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_aggregate_subscores"
					+ " (document_id, score, subscore, subscore_raw, subscore_normalized, subscore_count, subscore_rank, subscore_percentage, subscore_percentile)"
					+ " values (?,?,?,?,?,?,?,?,?)"
		    		+ "ON CONFLICT DO NOTHING";
			aggregateSubScoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_aggregate_lists"
					+ " (document_id, score, subscore, subscore_raw)"
					+ " values (?,?,?,?)"
		    		+ "ON CONFLICT DO NOTHING";
			aggregateListInsert = conn.prepareStatement(sql);
			Iterator<String> fieldsIter = in.fieldNames();
			while (fieldsIter.hasNext()) {
				String score = fieldsIter.next();
				if (in.get(score) != null && !in.get(score).isNull() && !score.equals("metadata")) {
					JsonNode aggregateScore = in.get(score);
					if (aggregateScore.isArray()) {
						List<PhraseAnnotation> typedScore = getMapper().convertValue(
								aggregateScore, 
								getMapper().getTypeFactory().constructCollectionType(List.class, PhraseAnnotation.class)
						);
						for (PhraseAnnotation phraseAnnotation : typedScore) {
							aggregateListInsert.setString(1, docId);
							aggregateListInsert.setString(2, score);
							aggregateListInsert.setString(3,
									phraseAnnotation.getName().substring(
											0, 
											phraseAnnotation.getName().length()>500?500:phraseAnnotation.getName().length()
									)
							);
							aggregateListInsert.setBigDecimal(4, phraseAnnotation.getValue());

							aggregateListInsert.executeUpdate();
						}
					}
					else if (aggregateScore.isObject()) {
						aggregateScoreInsert.setString(1, docId);
						aggregateScoreInsert.setString(2, score);
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
							aggregateSubScoreInsert.setString(2, score);
							aggregateSubScoreInsert.setString(3, subScore.get("name").asText());
							aggregateSubScoreInsert.setBigDecimal(4, subScore.get("score").get("raw").decimalValue());
							aggregateSubScoreInsert.setBigDecimal(5, subScore.get("score").get("normalized").decimalValue());
							aggregateSubScoreInsert.setBigDecimal(6, subScore.get("score").get("count").decimalValue());
							aggregateSubScoreInsert.setBigDecimal(7, subScore.get("aggregateScore").get("rank").decimalValue());
							aggregateSubScoreInsert.setBigDecimal(8, subScore.get("aggregateScore").get("percentage").decimalValue());
							aggregateSubScoreInsert.setBigDecimal(9, subScore.get("aggregateScore").get("percentile").decimalValue());
							//aggregateSubScoreInsert.executeUpdate();
							aggregateSubScoreInsert.addBatch();
						}
						aggregateSubScoreInsert.executeBatch();
					}
				}
			}
			conn.commit();
			conn.setAutoCommit(autoCommit);
        }


	    finally {
	    	PostgreSQLUtils.getInstance().closeFinally(pstmt);
	    	PostgreSQLUtils.getInstance().closeFinally(aggregateScoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(aggregateSubScoreInsert);
	    	PostgreSQLUtils.getInstance().closeFinally(conn);
	    }
	}
	

	@Override
	public ObjectNode getCorpusIDFScores(String corpus) throws Exception {
		String corpusSizeSql = "select count(distinct(document_id)) as corpus_size " + 
				"FROM submissions.document_aggregate_subscores ";
		String idfSql = "select score, subscore " + 
				", count(document_id) as document_count " + 
				"FROM submissions.document_aggregate_subscores " + 
				"group by score, subscore " +
				"order by score, subscore";
		ObjectNode retval = getMapper().createObjectNode();
		Connection conn = null;
		PreparedStatement corpusSizeSelect = null;
		ResultSet corpusSizeSelectCursor = null;
		PreparedStatement idfSelect = null;
		ResultSet idfSelectCursor = null;
		int corpusSize = 0;
        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    corpusSizeSelect = conn.prepareStatement(corpusSizeSql);
		    corpusSizeSelectCursor = corpusSizeSelect.executeQuery();
		    //just throw if no record
		    corpusSizeSelectCursor.next();
		    corpusSize = corpusSizeSelectCursor.getInt("corpus_size");
		    corpusSizeSelectCursor.close();
		    corpusSizeSelect.close();
		    
		    idfSelect = conn.prepareStatement(idfSql);
		    idfSelectCursor = idfSelect.executeQuery();
		    String prevScore = "";
		    ObjectNode scoreNode = getMapper().createObjectNode();
		    while (idfSelectCursor.next()) {
		    	String score = idfSelectCursor.getString("score");
		    	if (!score.equals(prevScore)) {
		    		//getLogger().debug(String.format("prev: %s new: %s", prevScore, score));
		    		scoreNode = retval.putObject(score);
		    		prevScore = score;
		    	}
		    	ObjectNode subscoreNode = scoreNode.putObject(idfSelectCursor.getString("subscore"));
		    	subscoreNode.put("documentCount", idfSelectCursor.getBigDecimal("document_count"));
		    	subscoreNode.put("corpusSize", new BigDecimal(corpusSize));
		    	//getLogger().debug(String.format("score: %s subscore %s", idfSelectCursor.getString("score"), idfSelectCursor.getString("subscore")));
		    	//getLogger().debug(getMapper().writeValueAsString(scoreNode));
		    }
		    retval.set(prevScore, scoreNode);
        }

        finally {
        	PostgreSQLUtils.getInstance().closeFinally(corpusSizeSelectCursor);
        	PostgreSQLUtils.getInstance().closeFinally(corpusSizeSelect);
        	PostgreSQLUtils.getInstance().closeFinally(idfSelectCursor);
        	PostgreSQLUtils.getInstance().closeFinally(idfSelect);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
        return retval;
		
	}
	
	@Override
	public ObjectNode getCorpusAggregateScores(String corpus) throws Exception {
		String aggregateScoreSql = "select score" + 
				", min(score_raw) as score_raw_min" + 
				", avg(score_raw) as score_raw_mean" + 
				", median(score_raw) as score_raw_median" + 
				", max(score_raw) as score_raw_max" + 
				", COALESCE(stddev(score_raw), 0) as score_raw_stddev" + 
				", min(score_normalized) as score_normalized_min" + 
				", avg(score_normalized) as score_normalized_mean" + 
				", median(score_normalized) as score_normalized_median" + 
				", max(score_normalized) as score_normalized_max" + 
				", COALESCE(stddev(score_normalized), 0) as score_normalized_stddev" + 
				", min(score_count) as score_count_min" + 
				", avg(score_count) as score_count_mean" + 
				", median(score_count) as score_count_median" + 
				", max(score_count) as score_count_max" + 
				", COALESCE(stddev(score_count), 0) as score_count_stddev" + 
				", min(score_min) as score_min_min" + 
				", avg(score_min) as score_min_mean" + 
				", median(score_min) as score_min_median" + 
				", max(score_min) as score_min_max" + 
				", COALESCE(stddev(score_min), 0) as score_min_stddev" + 
				", min(score_mean) as score_mean_min" + 
				", avg(score_mean) as score_mean_mean" + 
				", median(score_mean) as score_mean_median" + 
				", max(score_mean) as score_mean_max" + 
				", COALESCE(stddev(score_mean), 0) as score_mean_stddev" + 
				", min(score_median) as score_median_min" + 
				", avg(score_median) as score_median_mean" + 
				", median(score_median) as score_median_median" + 
				", max(score_median) as score_median_max" + 
				", COALESCE(stddev(score_median), 0) as score_median_stddev" + 
				", min(score_max) as score_max_min" + 
				", avg(score_max) as score_max_mean" + 
				", median(score_max) as score_max_median" + 
				", max(score_max) as score_max_max" + 
				", COALESCE(stddev(score_max), 0) as score_max_stddev" + 
				" from submissions.document_aggregate_scores group by score";
		
		String aggregateSubScoreSql = "select score, subscore" + 
				", min(subscore_raw) as subscore_raw_min" + 
				", avg(subscore_raw) as subscore_raw_mean" + 
				", median(subscore_raw) as subscore_raw_median" + 
				", max(subscore_raw) as subscore_raw_max" + 
				", COALESCE(stddev(subscore_raw), 0) as subscore_raw_stddev" + 
				", min(subscore_normalized) as subscore_normalized_min" + 
				", avg(subscore_normalized) as subscore_normalized_mean" + 
				", median(subscore_normalized) as subscore_normalized_median" + 
				", max(subscore_normalized) as subscore_normalized_max" + 
				", COALESCE(stddev(subscore_normalized), 0) as subscore_normalized_stddev" + 
				", min(subscore_count) as subscore_count_min" + 
				", avg(subscore_count) as subscore_count_mean" + 
				", median(subscore_count) as subscore_count_median" + 
				", max(subscore_count) as subscore_count_max" + 
				", COALESCE(stddev(subscore_count), 0) as subscore_count_stddev" + 
				", min(subscore_rank) as subscore_rank_min" + 
				", avg(subscore_rank) as subscore_rank_mean" + 
				", median(subscore_rank) as subscore_rank_median" + 
				", max(subscore_rank) as subscore_rank_max" + 
				", COALESCE(stddev(subscore_rank), 0) as subscore_rank_stddev" + 
				", min(subscore_percentage) as subscore_percentage_min" + 
				", avg(subscore_percentage) as subscore_percentage_mean" + 
				", median(subscore_percentage) as subscore_percentage_median" + 
				", max(subscore_percentage) as subscore_percentage_max" + 
				", COALESCE(stddev(subscore_percentage), 0) as subscore_percentage_stddev" + 
				", min(subscore_percentile) as subscore_percentile_min" + 
				", avg(subscore_percentile) as subscore_percentile_mean" + 
				", median(subscore_percentile) as subscore_percentile_median" + 
				", max(subscore_percentile) as subscore_percentile_max" + 
				", COALESCE(stddev(subscore_percentile), 0) as subscore_percentile_stddev" + 
				" from submissions.document_aggregate_subscores " +
				"group by score, subscore " +
				"order by score, subscore";
		
		ObjectNode retval = getMapper().createObjectNode();
		Connection conn = null;
		PreparedStatement aggregateScoreSelect = null;
		ResultSet aggregateScoreSelectCursor = null;
		PreparedStatement aggregateSubScoreSelect = null;
		ResultSet aggregateSubScoreSelectCursor = null;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    aggregateScoreSelect = conn.prepareStatement(aggregateScoreSql);
		    aggregateScoreSelectCursor = aggregateScoreSelect.executeQuery();
		    List<String> scoreMeasures = Arrays.asList("raw", "normalized", "count");
		    List<String> statsMeasures = Arrays.asList("min", "mean", "median", "max");
		    List<String> aggregateFunctions = Arrays.asList("min", "mean", "median", "max", "stddev");
		    while (aggregateScoreSelectCursor.next()) {
		    	String scoreName = aggregateScoreSelectCursor.getString("score");
		    	ObjectNode annotation = retval.putObject(scoreName);
		    	ObjectNode annotationScore = annotation.putObject("score");
		    	ObjectNode annotationStats = annotation.putObject("stats");
		    	for (String scoreMeasure : scoreMeasures) {
		    		ObjectNode annotationScoreScore = annotationScore.putObject(scoreMeasure);
		    		for (String aggregateFunction : aggregateFunctions) {
		    			annotationScoreScore.put(aggregateFunction,
		    					aggregateScoreSelectCursor.getBigDecimal("score_" + scoreMeasure + "_" + aggregateFunction)
		    			);
		    		}
		    		
		    	}
		    	for (String statsMeasure : statsMeasures) {
		    		ObjectNode annotationStatsScore = annotationStats.putObject(statsMeasure);
		    		for (String aggregateFunction : aggregateFunctions) {
		    			annotationStatsScore.put(aggregateFunction,
		    					aggregateScoreSelectCursor.getBigDecimal("score_" + statsMeasure + "_" + aggregateFunction)
		    			);
		    		}
		    		
		    	}	
		    }
		    aggregateScoreSelectCursor.close();
		    aggregateScoreSelect.close();
		    
		    aggregateSubScoreSelect = conn.prepareStatement(aggregateSubScoreSql);
		    aggregateSubScoreSelectCursor = aggregateSubScoreSelect.executeQuery();
		    scoreMeasures = Arrays.asList("raw", "normalized", "count");
		    statsMeasures = Arrays.asList("rank", "percentage", "percentile");
		    aggregateFunctions = Arrays.asList("min", "mean", "median", "max", "stddev");
		    String prevScoreName = "";
		    ObjectNode annotation = null;
		    ArrayNode aggregatedScores = null;
		    while (aggregateSubScoreSelectCursor.next()) {
		    	String scoreName = aggregateSubScoreSelectCursor.getString("score");
		    	String subscoreName = aggregateSubScoreSelectCursor.getString("subscore");
		    	if (!scoreName.equals(prevScoreName)) {
		    		annotation = (ObjectNode) retval.get(scoreName);
		    		prevScoreName = scoreName;
		    		aggregatedScores = annotation.putArray("aggregatedScores");
		    	}
		    	ObjectNode aggregatedScore = getMapper().createObjectNode();
		    	aggregatedScores.add(aggregatedScore);
		    	aggregatedScore.put("name", subscoreName);
		    	ObjectNode annotationScore = aggregatedScore.putObject("score");
		    	ObjectNode annotationStats = aggregatedScore.putObject("aggregateScore");
		    	for (String scoreMeasure : scoreMeasures) {
		    		ObjectNode annotationScoreScore = annotationScore.putObject(scoreMeasure);
		    		for (String aggregateFunction : aggregateFunctions) {
		    			annotationScoreScore.put(aggregateFunction,
		    					aggregateSubScoreSelectCursor.getBigDecimal("subscore_" + scoreMeasure + "_" + aggregateFunction)
		    			);
		    		}
		    		
		    	}
		    	for (String statsMeasure : statsMeasures) {
		    		ObjectNode annotationStatsScore = annotationStats.putObject(statsMeasure);
		    		for (String aggregateFunction : aggregateFunctions) {
		    			annotationStatsScore.put(aggregateFunction,
		    					aggregateSubScoreSelectCursor.getBigDecimal("subscore_" + statsMeasure + "_" + aggregateFunction)
		    			);
		    		}
		    		
		    	}
		    }
		    aggregateSubScoreSelectCursor.close();
		    aggregateSubScoreSelect.close();
		    
        }

        finally {
        	PostgreSQLUtils.getInstance().closeFinally(aggregateScoreSelectCursor);
        	PostgreSQLUtils.getInstance().closeFinally(aggregateScoreSelect);
        	PostgreSQLUtils.getInstance().closeFinally(aggregateSubScoreSelectCursor);
        	PostgreSQLUtils.getInstance().closeFinally(aggregateSubScoreSelect);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
        return retval;
		
	}

	@Override
	public void storeOOPZScores(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception {
		String insertScoresSql = "insert into document_aggregate_scores_z "
				+ "("
				+ "document_id"
				+ ", score"
				+ ", score_raw"
				+ ", score_normalized"
				+ ", score_count"
				+ ", score_min"
				+ ", score_max"
				+ ", score_mean"
				+ ", score_median"
				+ ")"
				+ " values "
				+ "("
				+ "?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ")"
				+ "ON CONFLICT DO NOTHING";
		String insertSubScoresSql = "insert into document_aggregate_subscores_z "
				+ "("
				+ "document_id"
				+ ", score"
				+ ", subscore"
				+ ", subscore_raw"
				+ ", subscore_normalized"
				+ ", subscore_count"
				+ ", subscore_rank"
				+ ", subscore_percentage"
				+ ", subscore_percentile"
				+ ")"
				+ " values "
				+ "("
				+ "?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ")"
				+ "ON CONFLICT DO NOTHING";

		Connection conn = null;
		PreparedStatement aggregateScoreInsert = null;
		PreparedStatement aggregateSubScoreInsert = null;
		boolean autoCommit = true;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
		    autoCommit = conn.getAutoCommit();
		    conn.setAutoCommit(false);
			aggregateScoreInsert = conn.prepareStatement(insertScoresSql);
			aggregateSubScoreInsert = conn.prepareStatement(insertSubScoresSql);
			ObjectNode documentAggregates = in;
			
			Iterator<String> annotationNameIter = documentAggregates.fieldNames();
			while (annotationNameIter.hasNext()) {
				String annotationName = annotationNameIter.next();
				if (documentAggregates.get(annotationName).isObject()) {
					if (!annotationName.equals("metadata")) {
						ObjectNode aggregateScore = (ObjectNode) documentAggregates.get(annotationName);
						aggregateScoreInsert.setString(1, scratchFileName);
						aggregateScoreInsert.setString(2, annotationName);
						aggregateScoreInsert.setBigDecimal(3, aggregateScore.get("scoreStats").get("score").get("raw").decimalValue());
						aggregateScoreInsert.setBigDecimal(4, aggregateScore.get("scoreStats").get("score").get("normalized").decimalValue());
						aggregateScoreInsert.setBigDecimal(5, aggregateScore.get("scoreStats").get("score").get("count").decimalValue());
						aggregateScoreInsert.setBigDecimal(6, aggregateScore.get("scoreStats").get("stats").get("min").decimalValue());
						aggregateScoreInsert.setBigDecimal(7, aggregateScore.get("scoreStats").get("stats").get("max").decimalValue());
						aggregateScoreInsert.setBigDecimal(8, aggregateScore.get("scoreStats").get("stats").get("mean").decimalValue());
						aggregateScoreInsert.setBigDecimal(9, aggregateScore.get("scoreStats").get("stats").get("median").decimalValue());
						aggregateScoreInsert.executeUpdate();
						
						for (JsonNode subScore : ((ArrayNode)aggregateScore.get("aggregatedScores"))) {
							aggregateSubScoreInsert.setString(1, scratchFileName);
							aggregateSubScoreInsert.setString(2, annotationName);
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
			conn.commit();
			conn.setAutoCommit(autoCommit);
        }

        finally {
        	PostgreSQLUtils.getInstance().closeFinally(aggregateScoreInsert);
        	PostgreSQLUtils.getInstance().closeFinally(aggregateSubScoreInsert);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
	}

	@Override
	public void storeOOPTfidfScores(String corpus, String stagingBatchName, String scratchFileName, ObjectNode in) throws Exception {
		String insertSubScoresSql = "insert into document_aggregate_subscores_tfidf "
				+ "("
				+ "document_id"
				+ ", score"
				+ ", subscore"
				+ ", subscore_tfidf"
				+ ")"
				+ " values "
				+ "("
				+ "?"
				+ ", ?"
				+ ", ?"
				+ ", ?"
				+ ")"
				+ "ON CONFLICT DO NOTHING";
		Connection conn = null;
		PreparedStatement aggregateSubScoreInsert = null;

        try {
		    conn = PostgreSQLUtils.getInstance().getClient(corpus);
			aggregateSubScoreInsert = conn.prepareStatement(insertSubScoresSql);
			ObjectNode documentAggregates = in;
			
			Iterator<String> annotationNameIter = documentAggregates.fieldNames();
			while (annotationNameIter.hasNext()) {
				String annotationName = annotationNameIter.next();
				if (documentAggregates.get(annotationName).isObject()) {
					if (!annotationName.equals("metadata")) {
						ArrayNode aggregatedScores = (ArrayNode) documentAggregates.get(annotationName).get("aggregatedScores");
						for (JsonNode aggregatedScore : aggregatedScores) {
							aggregateSubScoreInsert.setString(1, scratchFileName);
							aggregateSubScoreInsert.setString(2, annotationName);
							aggregateSubScoreInsert.setString(3, aggregatedScore.get("name").asText());
							aggregateSubScoreInsert.setBigDecimal(4, aggregatedScore.get("tfidf").decimalValue());
							aggregateSubScoreInsert.executeUpdate();
						}
					}
				}
			}
        }
        finally {
        	PostgreSQLUtils.getInstance().closeFinally(aggregateSubScoreInsert);
        	PostgreSQLUtils.getInstance().closeFinally(conn);
        }
	}

}
