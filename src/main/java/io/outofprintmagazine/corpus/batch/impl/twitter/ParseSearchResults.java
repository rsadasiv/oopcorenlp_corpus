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
package io.outofprintmagazine.corpus.batch.impl.twitter;

import java.io.IOException;
import java.util.List;

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
		//InputStream input = new FileInputStream("data/twitter.properties");
        //Properties props = new Properties();
        //props.load(input);
        //input.close();
		//Properties props = ParameterStore.getInstance().getProperties("data", "twitter.properties");
	    ConfigurationBuilder cb = new ConfigurationBuilder();
	    cb.setOAuthConsumerKey(getParameterStore().getProperty("twitter_apiKey"));
	    cb.setOAuthConsumerSecret(getParameterStore().getProperty("twitter_apiSecretKey"));
	    cb.setOAuthAccessToken(getParameterStore().getProperty("twitter_accessToken"));
	    cb.setOAuthAccessTokenSecret(getParameterStore().getProperty("twitter_accessTokenSecret"));

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
