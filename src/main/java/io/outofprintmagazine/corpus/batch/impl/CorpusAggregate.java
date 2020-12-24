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
package io.outofprintmagazine.corpus.batch.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.DeleteDbFiles;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.batch.ICorpusBatchStep;
import io.outofprintmagazine.nlp.pipeline.PhraseAnnotation;

public class CorpusAggregate extends CorpusBatchStep implements ICorpusBatchStep {
	
	private static final Logger logger = LogManager.getLogger(CorpusAggregate.class);

	@SuppressWarnings("unused")
	private Logger getLogger() {
		return logger;
	}
	
	public CorpusAggregate() {
		super();				
	}
	
	private Pattern isNumeric = Pattern.compile("\\d+");
	
	@Override
	public ArrayNode run(ArrayNode input)  {
		try {
			createSchema(getData().getCorpusId());
		}
		catch (Exception e) {
			getLogger().error(e);
		}
		ArrayNode retval = super.run(input);

		try {
			String storageLocation = getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("CORPUS_AGGREGATES", "json"),
					getCorpusAggregateScores(getData().getCorpusId())
				);
			String storageLocationIdf = getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("CORPUS_AGGREGATES_IDF", "json"),
					getCorpusIDFScores(getData().getCorpusId())
				);
			String storageLocationMb = getStorage().storeScratchFileObject(
					getData().getCorpusId(),
					getOutputScratchFilePath("CORPUS_AGGREGATES_MB", "json"),
					getCorpusMyersBriggsAggregateScores(getData().getCorpusId())
				);			
			FileInputStream fin = null;
			String storageLocationH2 = null;
			try {
				fin = new FileInputStream(new File(getH2Location(getData().getCorpusId())+".mv.db"));
				storageLocationH2 = getStorage().storeScratchFileStream(
					getData().getCorpusId(),
					getOutputScratchFilePath("CORPUS_AGGREGATES.mv.db"),
					fin
				);
			}
			finally {
				if (fin != null) {
					fin.close();
				}
			}
			for (JsonNode outputStepItem : retval) { 
				((ObjectNode)outputStepItem).put(
						"oopNLPCorpusAggregatesStorage",
						storageLocation
					);
				((ObjectNode)outputStepItem).put(
						"oopNLPCorpusIdfScoresStorage",
						storageLocationIdf
					);
				((ObjectNode)outputStepItem).put(
						"oopNLPCorpusMyersBriggsAggregateScoresStorage",
						storageLocationMb
					);						
				if (storageLocationH2 != null) {
					((ObjectNode)outputStepItem).put(
							"oopNLPCorpusAggregatesH2Storage",
							storageLocationH2
						);					
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			getLogger().error(e);
		}
		
		deleteH2(getData().getCorpusId());

		return retval;
		
	}
	
	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
		JsonNode aggregateNode = getJsonNodeFromStorage(inputStepItem, "oopNLPAggregatesStorage");
		if (aggregateNode != null && !aggregateNode.isNull()) {
			storeOOPAggregates(
				getData().getCorpusId(), 
				getDocID(inputStepItem),
				(ObjectNode) aggregateNode
			);
		}
				
		retval.add(outputStepItem);
		return retval;
	}
	
	private void deleteH2(String corpus) {
		//getClient(corpus).close();
		DeleteDbFiles.execute(System.getProperty("java.io.tmpdir"), corpus + "_h2", true);
	}
	
	private String getH2Location(String corpus) {
		//return "mem:" + corpus;
		return System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") +  corpus + "_h2";
		//return "C:\\Users\\rsada\\eclipse-workspace\\oopcorenlp\\data\\db\\" + corpus + "_h2";
	}
	
	private Connection getClient(String corpus) throws SQLException {
	    String DB_DRIVER = "org.h2.Driver";
        Connection dbConnection = null;
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection("jdbc:h2:" + getH2Location(corpus));
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return dbConnection;   
	}
	
	private void createSchema(String corpus) throws SQLException, IOException {
		Connection conn = null;
		Statement stmt = null;
        try {
        	conn = getClient(corpus);
		    stmt = conn.createStatement();
		    String[] ddl = 
		    		IOUtils.toString(
		    				getClass().getClassLoader().getResourceAsStream(
		    						"io/outofprintmagazine/corpus/storage/db/create_oopnlp_schema.sql"
		    				), 
		    				StandardCharsets.UTF_8.name()
		    		).split(";");
		    for (int i=0;i<ddl.length;i++) {
		    	if (ddl[i].trim().length() > 0) {
			    	try {
			    		stmt.execute(ddl[i]);		    		
			    	}
			    	catch (Exception e) {
			    		getLogger().debug(e);
			    	}
		    	}
		    }
		    stmt.close();
        }
        finally {
        	if (stmt != null) {
        		try {
        			stmt.close();
        		}
        		catch (Exception e) {
        			stmt = null;
        		}
        	}
        	if (conn != null) {
        		try {
        			conn.close();
        		}
        		catch (Exception e) {
        			conn = null;
        		}
        	}
        }
	}
	
	
	protected void storeOOPAggregates(String corpus, String docId, ObjectNode in) throws Exception {

		Connection conn = null;
		PreparedStatement documentDelete = null;
		PreparedStatement documentInsert = null;
		PreparedStatement scoreInsert = null;
		PreparedStatement aggregateScoreInsert = null;
		PreparedStatement aggregateListInsert = null;
		PreparedStatement aggregateSubScoreInsert = null;

        try {
        	conn = getClient(corpus);
        	String sql = "delete from documents where document_id = ?";
        	documentDelete = conn.prepareStatement(sql);
        	
        	sql = "insert into documents(document_id) values ?";
        	documentInsert = conn.prepareStatement(sql);
        	
        	sql = "insert into scores(score) values(?)";
        	scoreInsert = conn.prepareStatement(sql);
        	
		    sql = "insert into document_aggregate_scores"
					+ " (document_id, score, score_raw, score_normalized, score_count, score_min, score_max, score_mean, score_median, score_stddev)"
					+ " values (?,?,?,?,?,?,?,?,?,?) ";
		    		//+ "ON CONFLICT DO NOTHING";
			aggregateScoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_aggregate_subscores"
					+ " (document_id, score, subscore, subscore_raw, subscore_normalized, subscore_count, subscore_rank, subscore_percentage, subscore_percentile)"
					+ " values (?,?,?,?,?,?,?,?,?)";
		    		//+ "ON CONFLICT DO NOTHING";
			aggregateSubScoreInsert = conn.prepareStatement(sql);
			sql = "insert into document_aggregate_lists"
					+ " (document_id, score, subscore, subscore_raw)"
					+ " values (?,?,?,?)";
		    		//+ "ON CONFLICT DO NOTHING";
			documentDelete.setString(1, docId);
			documentDelete.executeUpdate();
			documentInsert.setString(1,  docId);
			documentInsert.executeUpdate();
			
			
			aggregateListInsert = conn.prepareStatement(sql);
			Iterator<String> fieldsIter = in.fieldNames();
			while (fieldsIter.hasNext()) {
				String score = fieldsIter.next();

				if (in.get(score) != null && !in.get(score).isNull() && !score.equals("metadata")) {
					try {
						scoreInsert.setString(1,  score);
						scoreInsert.executeUpdate();
					}
					catch (Exception e) {
						getLogger().trace(e);
					}
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
						aggregateScoreInsert.setBigDecimal(3, new BigDecimal(aggregateScore.get("scoreStats").get("score").get("raw").asText()));
						aggregateScoreInsert.setBigDecimal(4, new BigDecimal(aggregateScore.get("scoreStats").get("score").get("normalized").asText()));
						aggregateScoreInsert.setBigDecimal(5, new BigDecimal(aggregateScore.get("scoreStats").get("score").get("count").asText()));
						aggregateScoreInsert.setBigDecimal(6, new BigDecimal(aggregateScore.get("scoreStats").get("stats").get("min").asText()));
						aggregateScoreInsert.setBigDecimal(7, new BigDecimal(aggregateScore.get("scoreStats").get("stats").get("max").asText()));
						aggregateScoreInsert.setBigDecimal(8, new BigDecimal(aggregateScore.get("scoreStats").get("stats").get("mean").asText()));
						aggregateScoreInsert.setBigDecimal(9, new BigDecimal(aggregateScore.get("scoreStats").get("stats").get("median").asText()));
						aggregateScoreInsert.setBigDecimal(10, new BigDecimal(aggregateScore.get("scoreStats").get("stats").get("stddev").asText()));
						aggregateScoreInsert.executeUpdate();
						for (JsonNode subScore : ((ArrayNode)aggregateScore.get("aggregatedScores"))) {
							if (!isNumeric.matcher(subScore.get("name").asText()).matches()) {
								aggregateSubScoreInsert.setString(1, docId);
								aggregateSubScoreInsert.setString(2, score);
								aggregateSubScoreInsert.setString(3, subScore.get("name").asText());
								aggregateSubScoreInsert.setBigDecimal(4, new BigDecimal(subScore.get("score").get("raw").asText()));
								aggregateSubScoreInsert.setBigDecimal(5, new BigDecimal(subScore.get("score").get("normalized").asText()));
								aggregateSubScoreInsert.setBigDecimal(6, new BigDecimal(subScore.get("score").get("count").asText()));
								aggregateSubScoreInsert.setBigDecimal(7, new BigDecimal(subScore.get("aggregateScore").get("rank").asText()));
								aggregateSubScoreInsert.setBigDecimal(8, new BigDecimal(subScore.get("aggregateScore").get("percentage").asText()));
								aggregateSubScoreInsert.setBigDecimal(9, new BigDecimal(subScore.get("aggregateScore").get("percentile").asText()));
								aggregateSubScoreInsert.executeUpdate();
							}
						}
					}
				}
			}
        }
        catch (Exception e) {
        	e.printStackTrace();
        }


	    finally {
	    	aggregateScoreInsert.close();
	    	aggregateSubScoreInsert.close();
	    	conn.close();
	    }
	}
	
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
				" from document_aggregate_scores group by score";
		
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
				" from document_aggregate_subscores " +
				"group by score, subscore " +
				"order by score, subscore";
		
		ObjectNode retval = getMapper().createObjectNode();
		Connection conn = null;
		PreparedStatement aggregateScoreSelect = null;
		ResultSet aggregateScoreSelectCursor = null;
		PreparedStatement aggregateSubScoreSelect = null;
		ResultSet aggregateSubScoreSelectCursor = null;

        try {
		    conn = getClient(corpus);
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
        
        catch (Exception e) {
        	e.printStackTrace();
        	getLogger().error(e);
        }

        finally {
        	aggregateScoreSelectCursor.close();
        	aggregateScoreSelect.close();
        	aggregateSubScoreSelectCursor.close();
        	aggregateSubScoreSelect.close();
        	conn.close();
        }
        return retval;
		
	}
	
	public ObjectNode getCorpusIDFScores(String corpus) throws Exception {
		String corpusSizeSql = "select count(distinct(document_id)) as corpus_size " + 
				"FROM document_aggregate_subscores ";
		String idfSql = "select score, subscore " + 
				", count(document_id) as document_count " + 
				"FROM document_aggregate_subscores " + 
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
		    conn = getClient(corpus);
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
        	corpusSizeSelectCursor.close();
        	corpusSizeSelect.close();
        	idfSelectCursor.close();
        	idfSelect.close();
        	conn.close();
        }
        return retval;
		
	}

	public ObjectNode getCorpusMyersBriggsAggregateScores(String corpus) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet cursor = null;
		String sql = "SELECT subscore, avg(subscore_normalized) as subscore_normalized_avg " 
				+ "FROM document_aggregate_subscores " 
				+ "where score = 'OOPMyersBriggsAnnotation' " 
				+ "group by subscore";
		ObjectNode retval = getMapper().createObjectNode();
        try {
		    conn = getClient(corpus);
			pstmt = conn.prepareStatement(sql);
			cursor = pstmt.executeQuery();
			while (cursor.next()) {
				retval.put(cursor.getString("subscore"), cursor.getBigDecimal("subscore_normalized_avg"));
			}
        }
        finally {
        	cursor.close();
        	pstmt.close();
        	conn.close();
        }
        return retval;
	}

}
