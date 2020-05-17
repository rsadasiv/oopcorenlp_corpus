package io.outofprintmagazine.corpus.storage.postgresql;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PostgreSQLUtils {

	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(PostgreSQLUtils.class);
	private Properties props;

	private PoolingDataSource<PoolableConnection> ds = null;
         
	private PostgreSQLUtils() throws IOException, ClassNotFoundException {
		super();
		InputStream input = new FileInputStream("data/postgresql.properties");
        props = new Properties();
        props.load(input);
        input.close();
		Class.forName("org.postgresql.Driver");
		ConnectionFactory connectionFactory = 
				new DriverManagerConnectionFactory(
						String.format(
								props.getProperty("url"),
								props.getProperty("user"),
								props.getProperty("pwd")
						),
						null
		);
		PoolableConnectionFactory poolableConnectionFactory = 
				new PoolableConnectionFactory(
						connectionFactory, 
						null
		);
		ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
		poolableConnectionFactory.setPool(connectionPool);
		ds = new PoolingDataSource<>(connectionPool);

	}
	
	private static PostgreSQLUtils single_instance = null; 

    public static PostgreSQLUtils getInstance() throws IOException, ClassNotFoundException { 
        if (single_instance == null) 
            single_instance = new PostgreSQLUtils(); 
  
        return single_instance; 
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
