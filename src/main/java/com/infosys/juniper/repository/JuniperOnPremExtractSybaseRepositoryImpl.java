package com.infosys.juniper.repository;

import java.sql.Connection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

//import com.iig.gcp.extraction.syabse.controller.ExtractionController;
import com.infosys.juniper.dao.JuniperOnPremExtractSybaseDao;
import com.infosys.juniper.dto.ConnectionDto;
import com.infosys.juniper.dto.TempTableInfoDto;
import com.infosys.juniper.util.ConnectionUtils;
import com.infosys.juniper.util.MetadataDBConnectionUtils;

@Component
public class JuniperOnPremExtractSybaseRepositoryImpl implements JuniperOnPremExtractSybaseRepository {

	private static final Logger logger = LogManager.getLogger(JuniperOnPremExtractSybaseRepositoryImpl.class);
	
	@Autowired
	JuniperOnPremExtractSybaseDao Dao;
	@Override
	public String testSybaseConnection(ConnectionDto connDto) {
		Connection conn=null;
		try {
			if(connDto.getDbName()==null||connDto.getDbName().isEmpty()) {
				logger.error("Database Missing");
				return "Failed";
				
				//conn=ConnectionUtils.connectSybase(connDto.getHostName()+":"+connDto.getPort()+":"+connDto.getDbName(), connDto.getUserName(), connDto.getPassword());
			}else {
				System.out.println("Connecting to Source Database");
				conn=ConnectionUtils.connectSybase(connDto.getHostName()+":"+connDto.getPort(), connDto.getUserName(), connDto.getPassword());
				System.out.println("Connection established");
			}
			conn.close();
			
		}catch(Exception e) {
			
			return "Failed";
			
		}
	
		return "success";
	}
	
	@Override
	public String addSybaseConnectionDetails(ConnectionDto connDto) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			logger.error(e.getMessage());
			return "Failed to connect to Metadata database";
		}
		
		System.out.println("connection established to Metadata DB");
		return Dao.insertSybaseConnectionDetails(conn, connDto);
	}
	
	@Override
	public String updateOracleConnectionDetails(ConnectionDto connDto) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.updateOracleConnectionDetails(conn, connDto);
	}
	
	
	@Override
	public String deleteSybaseConnectionDetails(ConnectionDto connDto) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.deleteSybaseConnectionDetails(conn, connDto);
	}
	

	@Override
	public String editTempTableDetails(String feed_id, String src_type) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.deleteTempTableMetadata(conn, feed_id,src_type);
	}

	@Override
	public String addTempTableDetails(TempTableInfoDto tempTableInfoDto) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.insertTempTableMetadata(conn, tempTableInfoDto);
	}

	@Override
	public String metaDataValidate(String feed_sequence, String project_id) {
		Connection conn=null;
		try {
			System.out.println("MetadataValidate Repository");
			System.out.println("Getting ORACLE MD Connection!");
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.metadataValidate(conn,feed_sequence,project_id);
	}

	@Override
	public String updateAfterMetadataValidate(String feed_sequence, String project_id) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.updateAfterMetadataValidate(conn, feed_sequence, project_id);
	}

	@Override
	public String deleteTempTableMetadata(String feed_sequence, String project_id) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.deleteTempTableMetadata(conn, feed_sequence, project_id);
	}

}
