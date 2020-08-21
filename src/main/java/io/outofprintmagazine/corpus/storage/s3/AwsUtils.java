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
package io.outofprintmagazine.corpus.storage.s3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;

import io.outofprintmagazine.util.IParameterStore;


public class AwsUtils {

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(AwsUtils.class);
	private Properties props;

	private AwsUtils(IParameterStore parameterStore) throws IOException {
		super();
		//InputStream input = new FileInputStream("data/aws.properties");
        //props = new Properties();
        //props.load(input);
        //input.close();
		//props = IParameterStore.getInstance().getProperties("data", "aws.properties");
		props = new Properties();
		//TODO - NPE?
		props.setProperty("access_key_id", parameterStore.getProperty("aws_access_key_id"));
		props.setProperty("secret_key_id", parameterStore.getProperty("aws_secret_key_id"));
		
	}
	
	private static Map<IParameterStore, AwsUtils> instances = new HashMap<IParameterStore, AwsUtils>();
	
    public static AwsUtils getInstance(IParameterStore parameterStore) throws IOException { 
        if (instances.get(parameterStore) == null) {
        	AwsUtils instance = new AwsUtils(parameterStore);
            instances.put(parameterStore, instance);
        }
        return instances.get(parameterStore); 
    }
    
    public AWSCredentials getBasicCredentials() {
    	return new BasicAWSCredentials(props.getProperty("access_key_id"), props.getProperty("secret_key_id"));
    }
    
    public Regions getRegion() {
    	return Regions.US_EAST_1;
    }
}
