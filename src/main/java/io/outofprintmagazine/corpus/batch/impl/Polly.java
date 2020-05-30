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

import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.polly.AmazonPolly;
import com.amazonaws.services.polly.AmazonPollyClientBuilder;
import com.amazonaws.services.polly.model.DescribeVoicesRequest;
import com.amazonaws.services.polly.model.DescribeVoicesResult;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.Voice;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.CorpusBatchStep;
import io.outofprintmagazine.corpus.storage.s3.AwsUtils;

public class Polly extends CorpusBatchStep {

	
	private static final Logger logger = LogManager.getLogger(CoreNLP.class);

	@Override
	protected Logger getLogger() {
		return logger;
	}
	private AmazonPolly polly;
	private Voice voice;
	//private String defaultPath = "";
	//private String defaultBucket;
	private Properties properties = new Properties();
	
	public Polly() throws IOException {
		super();
		polly = AmazonPollyClientBuilder.standard()
			      .withCredentials(new AWSStaticCredentialsProvider(AwsUtils.getInstance(getParameterStore()).getBasicCredentials()))
			      .withRegion(AwsUtils.getInstance(getParameterStore()).getRegion())
			      .build();
		properties.setProperty("Bucket", getParameterStore().getProperty("s3_Bucket"));
		properties.setProperty("Path", getParameterStore().getProperty("s3_Path"));
		//S3BatchStorage b = new S3BatchStorage();
		//defaultPath = b.getDefaultPath() + "/";
		//defaultBucket = S3BatchStorage.defaultBucket;
	}

	@Override
	public ArrayNode run(ArrayNode input) {
		DescribeVoicesRequest describeVoicesRequest = new DescribeVoicesRequest()
				.withEngine(getData().getProperties().get("Engine").asText("standard"))
				.withLanguageCode(getData().getProperties().get("LanguageCode").asText("en-IN"));

		// Synchronously ask Amazon Polly to describe available TTS voices.
		DescribeVoicesResult describeVoicesResult = polly.describeVoices(describeVoicesRequest);
		for (Voice v : describeVoicesResult.getVoices()) {
			if (v.getName().equals(getData().getProperties().get("Name").asText("Aditi"))) {
				voice = v;
			}
		}
		return super.run(input);
	}


	@Override
	public ArrayNode runOne(ObjectNode input) throws Exception {
		ArrayNode retval = getMapper().createArrayNode();
		ObjectNode outputStepItem = copyInputToOutput(input);
		if (voice != null) {
			StartSpeechSynthesisTaskResult result = 
				polly.startSpeechSynthesisTask(
					new StartSpeechSynthesisTaskRequest()
						.withEngine(getData().getProperties().get("Engine").asText("standard"))
						.withLanguageCode(getData().getProperties().get("LanguageCode").asText("en-IN"))
						.withVoiceId(voice.getId())
						.withOutputFormat("mp3")
						.withOutputS3BucketName(properties.getProperty("Bucket"))
						.withOutputS3KeyPrefix(
								properties.getProperty("Path") + "/"
								+ 
								getData().getCorpusId() 
								+ "/" 
								+ getData().getCorpusBatchId()
								+ "/"
								+ getData().getCorpusBatchStepId()
								+ "/"
								+ getDocID(outputStepItem)
						)
						.withText(getTextDocumentFromStorage(input))
			);
			outputStepItem.put(
					"pollyStorage",
					result.getSynthesisTask().getOutputUri()
			);
			retval.add(outputStepItem);
		}
		return retval;
	}

}
