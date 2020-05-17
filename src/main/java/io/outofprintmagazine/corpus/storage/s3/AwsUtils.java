package io.outofprintmagazine.corpus.storage.s3;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;


public class AwsUtils {

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(AwsUtils.class);
	Properties props;

	private AwsUtils() throws IOException {
		super();
		InputStream input = new FileInputStream("data/aws.properties");
        props = new Properties();
        props.load(input);
        input.close();
	}
	
	private static AwsUtils single_instance = null; 

    public static AwsUtils getInstance() throws IOException { 
        if (single_instance == null) {
            single_instance = new AwsUtils(); 
        }

        return single_instance; 
    }
    
    public AWSCredentials getBasicCredentials() {
    	return new BasicAWSCredentials(props.getProperty("access_key_id"), props.getProperty("secret_key_id"));
    }
    
    public Regions getRegion() {
    	return Regions.US_EAST_1;
    }
    
    public String getCorpusBucket() {
    	return "oop-corpora";
    }

}
