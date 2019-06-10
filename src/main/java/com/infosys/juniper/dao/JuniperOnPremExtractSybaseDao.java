package com.infosys.juniper.dao;

import java.sql.Connection;

import com.infosys.juniper.dto.ConnectionDto;
import com.infosys.juniper.dto.TempTableInfoDto;

public interface JuniperOnPremExtractSybaseDao {

	
	public String insertSybaseConnectionDetails(Connection conn,ConnectionDto connDto);
	public String updateOracleConnectionDetails(Connection conn, ConnectionDto connDto);
	public String deleteSybaseConnectionDetails(Connection conn, ConnectionDto connDto);
	public String deleteTempTableMetadata(Connection conn, String feed_id, String src_type);
	public String insertTempTableMetadata(Connection conn, TempTableInfoDto tempTableInfoDto);
	public String metadataValidate(Connection conn, String feed_sequence, String project_id);
	public String updateAfterMetadataValidate(Connection conn,String feed_id,String src_type);
	
	
	
	
}
