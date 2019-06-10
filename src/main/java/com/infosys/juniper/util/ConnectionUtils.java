package com.infosys.juniper.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import com.infosys.juniper.constant.MetadataDBConstants;

public class ConnectionUtils {
	static Connection connection = null;
	
	
	 public static Connection connectSybase(String ipPortDb,String user,String password) throws SQLException {

		 
		 try {
				
				if (connection == null || connection.isClosed()) {
					Class.forName(MetadataDBConstants.SYBASE_DRIVER);
					
					String jdbc = "jdbc:sybase:Tds:"+ipPortDb;
					connection = DriverManager.getConnection(jdbc, user, password);
				}
		 } catch (Exception e) {
			   e.printStackTrace();
				throw new SQLException("Exception occured while connecting to Sybase database");
			}
	 
	
	 	System.out.println("connection succeeded");
		return connection;
	 
 }	 

}
