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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.outofprintmagazine.util.ParameterStore;


public class PostgreSQLUtils {

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(PostgreSQLUtils.class);
	private Properties props;

	private BasicDataSource ds = null;
         
	private PostgreSQLUtils(ParameterStore parameterStore) throws IOException, ClassNotFoundException {
		super();
		ds = new BasicDataSource();
		ds.setDriverClassName("org.postgresql.Driver");
		ds.setUrl(parameterStore.getProperty("postgresql_url"));
		ds.setUsername(parameterStore.getProperty("postgresql_user"));
		ds.setPassword(parameterStore.getProperty("postgresql_pwd"));
		ds.setInitialSize(50);
		ds.setPoolPreparedStatements(true);
		ds.setValidationQuery("select 1");
		ds.setMaxIdle(30);
		ds.setMaxWaitMillis(20000);
		ds.setRemoveAbandonedTimeout(120);
	}
	
	private static Map<ParameterStore, PostgreSQLUtils> instances = new HashMap<ParameterStore, PostgreSQLUtils>();
	
    
    public static PostgreSQLUtils getInstance(ParameterStore parameterStore) throws IOException, ClassNotFoundException { 
        if (instances.get(parameterStore) == null) {
        	PostgreSQLUtils instance = new PostgreSQLUtils(parameterStore);
            instances.put(parameterStore, instance);
        }
        return instances.get(parameterStore); 
    }
    
    public String getLoginRole() {
    	return props.getProperty("user");
    }
    
    public Connection getClient() throws SQLException {
    	return ds.getConnection();
	}
    
    public Connection getClient(String corpus) throws SQLException {
    	Connection conn = getClient();
    	conn.setSchema(corpus);
    	return conn;
	}
    
    public void closeFinally(Connection conn) {
    	try {
    		if (conn != null) {
    			conn.close();
    			conn = null;
    		}
    	}
    	catch (Exception e) {
    		conn = null;
    	}    	
    }
    
    public void closeFinally(PreparedStatement pstmt) {
    	try {
    		if (pstmt != null) {
    			pstmt.close();
    			pstmt = null;
    		}
    	}
    	catch (Exception e) {
    		pstmt = null;
    	}   	
    }
    
    public void closeFinally(Statement stmt) {
    	try {
    		if (stmt != null) {
    			stmt.close();
    			stmt = null;
    		}
    	}
    	catch (Exception e) {
    		stmt = null;
    	}   	
    }
    
    public void closeFinally(ResultSet cursor) {
    	try {
    		if (cursor != null) {
    			cursor.close();
    			cursor = null;
    		}
    	}
    	catch (Exception e) {

    		cursor = null;
    	}  	
    }
    
}
