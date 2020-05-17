package io.outofprintmagazine.corpus.batch.impl.twitter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class ParseSearchResults extends CorpusBatchStep {
	
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(ParseSearchResults.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	
	private Twitter twitter;
	
	public ParseSearchResults() throws IOException {
		InputStream input = new FileInputStream("data/twitter.properties");
        Properties props = new Properties();
        props.load(input);
        input.close();
	    ConfigurationBuilder cb = new ConfigurationBuilder();
	    cb.setOAuthConsumerKey(props.getProperty("apiKey"));
	    cb.setOAuthConsumerSecret(props.getProperty("apiSecretKey"));
	    cb.setOAuthAccessToken(props.getProperty("accessToken"));
	    cb.setOAuthAccessTokenSecret(props.getProperty("accessTokenSecret"));

	    twitter = new TwitterFactory(cb.build()).getInstance();
	}

	@Override
	public ArrayNode runOne(ObjectNode inputStepItem) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();

	    //Query query = new Query("#twitterature");
		Query query = new Query(inputStepItem.get("q").asText());
	    QueryResult result;
	    do {
			result = twitter.search(query);
			List<Status> tweets = result.getTweets();
			for (Status tweet : tweets) {
				if (tweet.getLang().equals("en")) {
					ObjectNode outputStepItem = copyInputToOutput(inputStepItem);
					setAuthor(tweet.getUser().getName(), outputStepItem);
					setDate(getDateFormat().format(tweet.getCreatedAt()), outputStepItem);
					setTitle(tweet.getUser().getScreenName(), outputStepItem);
					setThumbnail(tweet.getUser().getProfileImageURL(), outputStepItem);
					setLink(tweet.getSource(), outputStepItem);
					outputStepItem.put("id", tweet.getId());
					setStorageLink(
							getStorage().storeScratchFileString(
								inputStepItem.get("corpusId").asText(), 
								getOutputScratchFilePath(new Long(tweet.getId()).toString(), "txt"),
								tweet.getText().trim()
							),
							outputStepItem
					);
					retval.add(outputStepItem);
				}
			}
	    } 
	    while ((query = result.nextQuery()) != null);
	    return retval;
	}
}
