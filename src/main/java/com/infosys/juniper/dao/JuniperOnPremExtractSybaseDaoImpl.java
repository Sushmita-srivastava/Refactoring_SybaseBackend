package com.infosys.juniper.dao;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.crypto.SecretKey;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.infosys.juniper.constant.EncryptionConstants;
import com.infosys.juniper.constant.MetadataDBConstants;
import com.infosys.juniper.dto.ConnectionDto;
import com.infosys.juniper.dto.TempTableInfoDto;
import com.infosys.juniper.dto.TempTableMetadataDto;
import com.infosys.juniper.repository.JuniperOnPremExtractSybaseRepository;
import com.infosys.juniper.util.ConnectionUtils;
import com.infosys.juniper.util.EncryptUtils;

import ch.qos.logback.classic.Logger;

@Component
public class JuniperOnPremExtractSybaseDaoImpl implements JuniperOnPremExtractSybaseDao {
	
	@Autowired
	JuniperOnPremExtractSybaseRepository repository;
	
	private static String masterKeyPath;
	@SuppressWarnings("static-access")
	@Value("${master.key.path}")
	public void setMasterKeyPath(String value) {
		this.masterKeyPath=value;
	}
	
	
	@Override
	public String insertSybaseConnectionDetails(Connection conn, ConnectionDto dto) {
		
		int systemSequence=0;
		int projectSequence=0;

		try {
			systemSequence=getSystemSequence(conn,dto.getSystem());
			projectSequence=getProjectSequence(conn,dto.getProject());
			
			System.out.println("system sequence is "+systemSequence+ " project sequence is "+projectSequence);
		}catch(SQLException e) {
			//logger.error(e.getMessage());
			
			e.printStackTrace();
			return "Error retrieving system and project details";
		}
		String insertConnDetails="";
		String sequence="";
		String connectionId="";
		byte[] encryptedKey=null;
		byte[] encryptedPassword=null;
		if(systemSequence!=0 && projectSequence!=0) {
			
			try {
				encryptedKey=getEncryptedKey(conn,systemSequence,projectSequence);
			}catch(Exception e) {
				e.printStackTrace();
				return "Error occured while fetching encryption key"; 
			}
			try {
				encryptedPassword=encryptPassword(encryptedKey,dto.getPassword());
			}catch(Exception e) {
				e.printStackTrace();
				return "Error occurred while encrypting password";
			}
			PreparedStatement pstm=null;
			
			try {
				insertConnDetails="insert into "+MetadataDBConstants.CONNECTIONTABLE+
						"(src_conn_name,src_conn_type,host_name,port_no,"
						+ "username,password,encrypted_encr_key,database_name,"
						+ "system_sequence,project_sequence,created_by) "
						+ "values(?,?,?,?,?,?,?,?,?,?,?)";
				pstm = conn.prepareStatement(insertConnDetails);
				pstm.setString(1, dto.getConn_name());
				pstm.setString(2, dto.getConn_type());
				pstm.setString(3, dto.getHostName());
				pstm.setString(4, dto.getPort());
				pstm.setString(5, dto.getUserName());
				pstm.setBytes(6,encryptedPassword);
				pstm.setBytes(7,encryptedKey);
				pstm.setString(8, dto.getDbName());
				//pstm.setString(9, dto.getServiceName());
				pstm.setInt(9, systemSequence);
				pstm.setInt(10, projectSequence);
				pstm.setString(11, dto.getJuniper_user());
				pstm.executeUpdate();
				pstm.close();
			}catch(Exception e) {
				e.printStackTrace();
				return e.getMessage();
			}
			
			try {	
				Statement statement=conn.createStatement();
				String query=MetadataDBConstants.GETSEQUENCEID.replace("${tableName}", MetadataDBConstants.CONNECTIONTABLE).replace("${columnName}", MetadataDBConstants.CONNECTIONTABLEKEY);
				ResultSet rs=statement.executeQuery(query);
				if(rs.isBeforeFirst()){
					rs.next();
					sequence=rs.getString(1).split("\\.")[1];
					rs=statement.executeQuery(MetadataDBConstants.GETLASTROWID.replace("${id}", sequence));
					if(rs.isBeforeFirst()){
						rs.next();
						connectionId=rs.getString(1);
					}
				}	
			}catch(Exception e) {
				e.printStackTrace();
				return e.getMessage();
			}finally {
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return "Execption ocurred while closing connection";
				}
			}

			return "success:"+connectionId;
		}else {
			return "System/Project not found";
		}
	}
	
	
	@Override
	public String updateOracleConnectionDetails(Connection conn, ConnectionDto connDto) {
		String updateConnectionMaster="";
		PreparedStatement pstm=null;
		int systemSequence=0;
		int projectSequence=0;
		byte[] encryptedKey=null;
		byte[] encryptedPassword=null;

		try {
			systemSequence=getSystemSequence(conn,connDto.getSystem());
			projectSequence=getProjectSequence(conn,connDto.getProject());
		}catch(SQLException e) {
			e.printStackTrace();
			return "Error while retrieving system or project Details";
		}
		
		if(systemSequence!=0 && projectSequence!=0) {
			
				if(!(connDto.getPassword()==null||connDto.getPassword().isEmpty())) 
				{
					try {
						encryptedKey=getEncryptedKey(conn,systemSequence,projectSequence);
					}catch(Exception e) {
						e.printStackTrace();
						return "Error occured while fetching encryption key"; 
					}
					try {
						encryptedPassword=encryptPassword(encryptedKey,connDto.getPassword());
					}catch(Exception e) {
						e.printStackTrace();
						return "Error occurred while encrypting password";
					}					
				}
				else {
					return "Password can not be Null";
				}

				updateConnectionMaster="update "+MetadataDBConstants.CONNECTIONTABLE
						+" set src_conn_name=?"+MetadataDBConstants.COMMA
						+"src_conn_type=?"+MetadataDBConstants.COMMA
						+"host_name=?"+MetadataDBConstants.COMMA
						+"port_no=?"+MetadataDBConstants.COMMA
						+"username=?"+MetadataDBConstants.COMMA
						+"password=?"+MetadataDBConstants.COMMA 
						+"encrypted_encr_key=?"+MetadataDBConstants.COMMA
						+"database_name=?"+MetadataDBConstants.COMMA
						//+"service_name=?"+MetadataDBConstants.COMMA
						+"system_sequence=?"+MetadataDBConstants.COMMA
						+"project_sequence=?"+MetadataDBConstants.COMMA
						+"updated_by=?"
						+" where src_conn_sequence="+connDto.getConnId();
				
				try {	
					pstm = conn.prepareStatement(updateConnectionMaster);
					pstm.setString(1, connDto.getConn_name());
					pstm.setString(2, connDto.getConn_type());
					pstm.setString(3, connDto.getHostName());
					pstm.setString(4, connDto.getPort());
					pstm.setString(5, connDto.getUserName());
					pstm.setBytes(6,encryptedPassword);
					pstm.setBytes(7,encryptedKey);
					pstm.setString(8, connDto.getDbName());
					//pstm.setString(9, connDto.getServiceName());
					pstm.setInt(9, systemSequence);
					pstm.setInt(10, projectSequence);
					pstm.setString(11, connDto.getJuniper_user());
					
					pstm.executeUpdate();
					pstm.close();
					return "Success";

				}catch (SQLException e) {

					return e.getMessage();


				}finally {
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						return "Execption ocurred while closing connection";
					}
				}
			

	
			
		}else{
			
			return "System or Project does not Exist";
		}

	}
	
	
	
	
	
	@Override
	public String deleteSybaseConnectionDetails(Connection conn, ConnectionDto connDto) {
		String deleteConnectionMaster="";
		PreparedStatement pstm=null;
		int systemSequence=0;
		int projectSequence=0;
		byte[] encryptedKey=null;
		byte[] encryptedPassword=null;

		try {
			systemSequence=getSystemSequence(conn,connDto.getSystem());
			projectSequence=getProjectSequence(conn,connDto.getProject());
		}catch(SQLException e) {
			e.printStackTrace();
			return "Error while retrieving system or project Details";
		}
		
		if(systemSequence!=0 && projectSequence!=0) {
			
				if(!(connDto.getPassword()==null||connDto.getPassword().isEmpty())) 
				{
					try {
						encryptedKey=getEncryptedKey(conn,systemSequence,projectSequence);
					}catch(Exception e) {
						e.printStackTrace();
						return "Error occured while fetching encryption key"; 
					}
					try {
						encryptedPassword=encryptPassword(encryptedKey,connDto.getPassword());
					}catch(Exception e) {
						e.printStackTrace();
						return "Error occurred while encrypting password";
					}					
				}
				else {
					return "Password can not be Null";
				}

				deleteConnectionMaster="delete from "+MetadataDBConstants.CONNECTIONTABLE
						+" where src_conn_sequence=?";
//						+" set src_conn_name=?"+MetadataDBConstants.COMMA
//						+"src_conn_type=?"+MetadataDBConstants.COMMA
//						+"host_name=?"+MetadataDBConstants.COMMA
//						+"port_no=?"+MetadataDBConstants.COMMA
//						+"username=?"+MetadataDBConstants.COMMA
//						+"password=?"+MetadataDBConstants.COMMA 
//						+"encrypted_encr_key=?"+MetadataDBConstants.COMMA
//						+"database_name=?"+MetadataDBConstants.COMMA
//						//+"service_name=?"+MetadataDBConstants.COMMA
//						+"system_sequence=?"+MetadataDBConstants.COMMA
//						+"project_sequence=?"+MetadataDBConstants.COMMA
//						+"updated_by=?";
						
				
				try {	
					pstm = conn.prepareStatement(deleteConnectionMaster);
					pstm.setString(1, String.valueOf(connDto.getConnId()));
//					pstm.setString(2, connDto.getConn_type());
//					pstm.setString(3, connDto.getHostName());
//					pstm.setString(4, connDto.getPort());
//					pstm.setString(5, connDto.getUserName());
//					pstm.setBytes(6,encrypted_password);
//					pstm.setBytes(7,encrypted_key);
//					pstm.setString(8, connDto.getDbName());
//					//pstm.setString(9, connDto.getServiceName());
//					pstm.setInt(10, system_sequence);
//					pstm.setInt(11, project_sequence);
//					pstm.setString(12, connDto.getJuniper_user());
					
					pstm.executeUpdate();
					pstm.close();
					return "Success";

				}catch (SQLException e) {

					return e.getMessage();


				}finally {
					try {
						conn.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						return "Execption ocurred while closing connection";
					}
				}
			

	
			
		}else{
			
			return "System or Project does not Exist";
		}

	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	private int getSystemSequence(Connection conn, String systemName) throws SQLException {
		// TODO Auto-generated method stub
		String query="select system_sequence from "+MetadataDBConstants.SYSTEMTABLE+" where system_name='"+systemName+"'";
		int sysSeq=0;
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			sysSeq=rs.getInt(1);

		}
		
		return sysSeq;

	}

	private int getProjectSequence(Connection conn, String project) throws SQLException {
		// TODO Auto-generated method stub
		String query="select project_sequence from "+MetadataDBConstants.PROJECTTABLE+" where project_id='"+project+"'";
		int proj_seq=0;
		Statement statement=conn.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if(rs.isBeforeFirst()) {

			rs.next();
			proj_seq=rs.getInt(1);


		}
		
		return proj_seq;
	}
	
	@SuppressWarnings("unchecked")
	private byte[] getEncryptedKey(Connection conn,int system_sequence, int project_sequence) throws Exception {

		
		JSONObject json=new JSONObject();
		json.put("system", Integer.toString(system_sequence));
		json.put("project", Integer.toString(project_sequence));
		
			System.out.println("calling encryption service");
			String response=invokeEncryption(json,EncryptionConstants.ENCRYPTIONSERVICEURL);
			System.out.println("response is "+response);
			JSONObject jsonResponse = (JSONObject) new JSONParser().parse(response);
			if(jsonResponse.get("status").toString().equalsIgnoreCase("FAILED")) {
				throw new Exception("Error ocurred while retrieving encryption key");
			}
			else {

				String query="select key_value from "+MetadataDBConstants.KEYTABLE+" where system_sequence="+system_sequence+
						" and project_sequence="+project_sequence;
				byte[] encryptedKey=null;
				Statement statement=conn.createStatement();
				ResultSet rs = statement.executeQuery(query);
				if(rs.isBeforeFirst()) {

					rs.next();
					encryptedKey=rs.getBytes(1);
					return encryptedKey;

				}
				else {
					throw new Exception("Key not Found");
				}

			}

		}
	
	
	private  String invokeEncryption(JSONObject json,String  url) throws UnsupportedOperationException, Exception {



		CloseableHttpClient httpClient = HttpClientBuilder.create().build();


		HttpPost postRequest=new HttpPost(url);
		postRequest.setHeader("Content-Type","application/json");
		StringEntity input = new StringEntity(json.toString());
		postRequest.setEntity(input); 
		HttpResponse response = httpClient.execute(postRequest);
		HttpEntity respEntity = response.getEntity();
		return EntityUtils.toString(respEntity);
	}
	
	private byte[] encryptPassword(byte[] encryptedKey, String password) throws Exception {

	
			String content = EncryptUtils.readFile(masterKeyPath);
			SecretKey secKey = EncryptUtils.decodeKeyFromString(content);
			String decryptedKey=EncryptUtils.decryptText(encryptedKey,secKey);
			byte[] encryptedPassword=EncryptUtils.encryptText(password,decryptedKey);
			return encryptedPassword;


		
	}


	@Override
	public String deleteTempTableMetadata(Connection conn, String feedId, String srcType) {
		String result="success";
		
		String selectTempTableMaster= "select * from "
				+MetadataDBConstants.TEMPTABLEDETAILSTABLE
				+" where feed_sequence="
				+feedId;
	
		String deleteTempTableMaster= "delete from "
									+MetadataDBConstants.TEMPTABLEDETAILSTABLE
									+" where feed_sequence="
									+feedId;
			
		try {	
			Statement statementSelectTemp = conn.createStatement();
			Statement statementDeleteTemp = conn.createStatement();
			ResultSet rsSelectTemp=statementSelectTemp.executeQuery(selectTempTableMaster);
			if(rsSelectTemp.next()) {
				ResultSet rsdeleteTemp=statementDeleteTemp.executeQuery(deleteTempTableMaster);
				if(rsdeleteTemp.next()) {
				result="successfully deleted the temp table records for feed_sequence"+feedId;
				}else {
					result="Failed to delete the records from the temp table for feed_sequence"+feedId;
				}
			}
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			result=e.getMessage();


		}finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return result;	

	}


	@Override
	public String insertTempTableMetadata(Connection conn, TempTableInfoDto tempTableInfoDto) {

		int projectSequence=0;
		Statement statement=null;
		try {
			statement=conn.createStatement();
			projectSequence=getProjectSequence(conn, tempTableInfoDto.getProject());
			if(projectSequence!=0) {
				for(TempTableMetadataDto tempTableMetadata:tempTableInfoDto.getTempTableMetadataArr()) {

					String columns="";
					if(tempTableMetadata.getColumns().equalsIgnoreCase("*")) {
						columns="all";
					}
					else {
						columns=tempTableMetadata.getColumns();
					}
					String insertTableMaster= MetadataDBConstants.INSERTQUERY.replace("{$table}", MetadataDBConstants.TEMPTABLEDETAILSTABLE)
							.replace("{$columns}","feed_sequence,table_name,columns,fetch_type,where_clause,incr_col,view_flag,view_source_schema,project_sequence,created_by" )
							.replace("{$data}",tempTableInfoDto.getFeed_id() +MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getTable_name()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+columns+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getFetch_type()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getWhere_clause()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getIncr_col()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getView_flag()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableMetadata.getView_source_schema()+MetadataDBConstants.QUOTE+MetadataDBConstants.COMMA
									+projectSequence+MetadataDBConstants.COMMA
									+MetadataDBConstants.QUOTE+tempTableInfoDto.getJuniper_user()+MetadataDBConstants.QUOTE
									);
					
					statement.executeUpdate(insertTableMaster);
					
				}
				return "success";
				
			}else {
				return "Project Details invalid";
			}
			
		}catch(Exception e) {
			return e.getMessage();
		}finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		

	}


	@Override
	public String metadataValidate(Connection conn, String feedSequence, String projectId) {
		String connectionType="";
		int counter=0;
		String host="";
		String port="";
		String serviceName="";
		String databaseName="";
		String user="";
		byte[] encryptedPassword=null;
		byte[] encryptedKey=null;
		Connection sourceConn=null;
		int count = 0;
		System.out.println("Reached inside validate 3");
		String query="select src_conn_type from "+MetadataDBConstants.CONNECTIONTABLE+
				" where src_conn_sequence=(select distinct src_conn_sequence from "+MetadataDBConstants.FEEDSRCTGTLINKTABLE
				+" where feed_sequence="+feedSequence+")";
		
		System.out.println(query);
		Statement statement=null;
		
		
		try {
			
			statement=conn.createStatement();
			ResultSet rs=statement.executeQuery(query);
			if(rs.next()) {
				connectionType=rs.getString(1);
				System.out.println("connection_type is "+connectionType);
				if(connectionType.equalsIgnoreCase("SYBASE")) {
					String sourceConnectionDetails="select host_name,port_no,username,password,database_name,service_name,ENCRYPTED_ENCR_KEY from "+MetadataDBConstants.CONNECTIONTABLE+
								" where src_conn_sequence=(select distinct src_conn_sequence from "+MetadataDBConstants.FEEDSRCTGTLINKTABLE
								+" where feed_sequence="+feedSequence+")";
					System.out.println("source_connection_details is "+sourceConnectionDetails);
					statement=conn.createStatement();
					ResultSet connRs=statement.executeQuery(sourceConnectionDetails);
					if(connRs.next()) {
						host=connRs.getString(1);
						port=connRs.getString(2);
						serviceName=connRs.getString(6);
						databaseName=connRs.getString(5);
						user=connRs.getString(3);
						encryptedPassword=connRs.getBytes(4);
						encryptedKey=connRs.getBytes(7);
						String password=null;
						password = decyptPassword(encryptedKey, encryptedPassword);
						String ORACLE_IP_PORT_SID=null;
//						if(service_name!=null) {
							
						if(databaseName!=null) {
							System.out.println();
							ORACLE_IP_PORT_SID=host+":"+port+"/"+databaseName;
							
						}else {
							ORACLE_IP_PORT_SID=host+":"+port+":"+serviceName;
						}
						 
						
						sourceConn=ConnectionUtils.connectSybase(ORACLE_IP_PORT_SID, user, password);
						Statement source_conn_statement=sourceConn.createStatement();
						String query2="select TABLE_NAME,COLUMNS,WHERE_CLAUSE,INCR_COL from "
									+MetadataDBConstants.TEMPTABLEDETAILSTABLE
									+" where feed_sequence="+feedSequence 
									+" and project_sequence="
									+"(select project_sequence from JUNIPER_PROJECT_MASTER where project_id='"
									+projectId+"')";
							System.out.println("query2 is "+query2);
							Statement fetchStatement=conn.createStatement();
							ResultSet rs1 = fetchStatement.executeQuery(query2);
							while(rs1.next()) {
								
								count++;
								
								System.out.println("Reached inside the while loop");
								String tableName=rs1.getString("TABLE_NAME").replace(".", "..");
								String columns=rs1.getString("COLUMNS");
								String whereClause=rs1.getString("WHERE_CLAUSE");
								String incrCol=rs1.getString("INCR_COL");
								if (columns.equalsIgnoreCase("all") && incrCol.equalsIgnoreCase("null")) {
//									query="explain plan for select *"+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
									query="explain select *"+" from "+tableName+" where "+whereClause;
								}else if (columns.equalsIgnoreCase("all") && incrCol != "null") {
//									query="explain plan for select "+INCR_COL+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
									query="explain select "+incrCol+" from "+tableName+" where "+whereClause;
								}else if (columns != "all" && incrCol.equalsIgnoreCase("null")){
//									query="explain plan for select "+COLUMNS+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
									query="explain select "+columns+" from "+tableName+" where "+whereClause;
								}else {
//									query="explain plan for select "+COLUMNS+","+INCR_COL+" from "+TABLE_NAME+" where "+WHERE_CLAUSE;
									query="explain select "+columns+","+incrCol+" from "+tableName+" where "+whereClause;
								}
								
								System.out.println("explain query is "+ query);
								//System.out.println("source_conn_statement.executeQuery(query) : "+source_conn_statement.executeQuery(query));
								System.out.println("Reached inside the validate block 4");
								query = "update "+MetadataDBConstants.TEMPTABLEDETAILSTABLE+" SET VALIDATION_FLAG='Y' where feed_sequence="+feedSequence+" and TABLE_NAME='"+tableName+"' and COLUMNS='"+columns+"' and INCR_COL='"+incrCol+"'";
								System.out.println("query is "+query);
								Statement updateStatement=conn.createStatement();
								
								System.out.println("Executing update >>"+ query);
								ResultSet updateStat= updateStatement.executeQuery(query);
								System.out.println("Executed update >>"+ query);
								System.out.println("Flag updated successfully for "+tableName);
								
								System.out.println("Count >>"+ count);
								
							}
							
						
					}
					
					if (counter==0) {
						String deleteMainResponse=null;
						deleteMainResponse = deleteTableMetadata(conn,feedSequence,projectId);
						if(deleteMainResponse.contains("success")) {
								String response=null;
								response = repository.updateAfterMetadataValidate(feedSequence,projectId);
								if(response.contains("success")) {
									String delete_temp_response=repository.deleteTempTableMetadata(feedSequence,projectId);
									if(delete_temp_response.contains("success")) {
										return "Metadata Validation successfull";		
									}else {
										
										return "The records not deleted from the primary table";
									}
								}else {
									
									return "Data not added in the final table";
								}
						}else{
							return "Failed to remove the records from the juniper_ext_table_master table";
						}
					}else {
						
						return "Metadata Validation failed";
					}
					
					
				}
			
			}
			return null;
		}catch(Exception e) {
			return e.getMessage();
		}finally {
			try {
				conn.close();
				sourceConn.close();
			}catch(Exception e) {
				e.printStackTrace();
			}
			
			
		}
		
		
		
	
	}
	
	
	private String decyptPassword(byte[] encryptedKey, byte[] encryptedPassword) throws Exception {

		String content = EncryptUtils.readFile(masterKeyPath);
		SecretKey secKey = EncryptUtils.decodeKeyFromString(content);
		String decryptedKey=EncryptUtils.decryptText(encryptedKey,secKey);
		SecretKey secKey2 = EncryptUtils.decodeKeyFromString(decryptedKey);
		String password=EncryptUtils.decryptText(encryptedPassword,secKey2);
		return password;

	}
	
	
	private String deleteTableMetadata(Connection conn,String feedId,String srcType) throws SQLException{
		String result="success";
		
		
		String selectTableMaster= "select * from "
		+MetadataDBConstants.TABLEDETAILSTABLE
		+" where feed_sequence="
		+feedId;
		String deleteTableMaster= "delete from "
				+MetadataDBConstants.TABLEDETAILSTABLE
				+" where feed_sequence="
				+feedId;
									
		try {	
			Statement statementSelectMain = conn.createStatement();
			Statement statementDeleteMain = conn.createStatement();
			
			ResultSet rsSelectMain=statementSelectMain.executeQuery(selectTableMaster);
			if(rsSelectMain.next()) {
				ResultSet rsDeleteMain=statementDeleteMain.executeQuery(deleteTableMaster);
				if(rsDeleteMain.next()) {
				result="successfully deleted the main table records for feed_sequence"+feedId;
				}else {
					result="Failed to delete the records from the main table for feed_sequence"+feedId;
				}
			}

		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//TODO: Log the error message
			conn.close();
			result=e.getMessage();


		}
		
		return result;
		}

	


	@Override
	public String updateAfterMetadataValidate(Connection conn,String feed_id,String src_type) {
		String result="";
		String feedSequence="";
		String tableName="";
		String columns="";
		String whereClause="";
		String fetchType="";
		String incrCol="";
		String viewFlag="";
		String viewSourceSchema="";
		String projectSequence="";
		String createdBy="";
		String updatedBy="";
		
					
					String distinctRecords= "select distinct feed_sequence,TABLE_NAME,COLUMNS,WHERE_CLAUSE,FETCH_TYPE,INCR_COL,VIEW_FLAG," + 
							"VIEW_SOURCE_SCHEMA,PROJECT_SEQUENCE,CREATED_BY,UPDATED_BY " + 
							"from JUNIPER_EXT_TABLE_MASTER_TEMP " + 
							"where feed_sequence="+feed_id +
							" and validation_flag ='Y'";
		
					System.out.println(distinctRecords);
												
					try {	
						Statement statement = conn.createStatement();
						Statement statement2 = conn.createStatement();
						ResultSet rs=statement.executeQuery(distinctRecords);
						while(rs.next()) {
							feedSequence=rs.getString("feed_sequence");
							tableName=rs.getString("TABLE_NAME");			
							columns=rs.getString("COLUMNS");
							whereClause=rs.getString("WHERE_CLAUSE");
							fetchType=rs.getString("FETCH_TYPE");
							incrCol=rs.getString("INCR_COL");
							viewFlag=rs.getString("VIEW_FLAG");
							viewSourceSchema=rs.getString("VIEW_SOURCE_SCHEMA");
							projectSequence=rs.getString("PROJECT_SEQUENCE");
							createdBy=rs.getString("CREATED_BY");
							updatedBy=rs.getString("UPDATED_BY");	
							String insertRecords ="insert into JUNIPER_EXT_TABLE_MASTER (feed_sequence,TABLE_NAME,COLUMNS,WHERE_CLAUSE,FETCH_TYPE,INCR_COL,VIEW_FLAG,VIEW_SOURCE_SCHEMA,PROJECT_SEQUENCE,CREATED_BY,UPDATED_BY)"
									+"values("+feedSequence
									+",'"+tableName+"','"+columns+"','"+whereClause+"','"+fetchType
									+"','"+incrCol
									+"','"+viewFlag
									+"','"+viewSourceSchema
									+"',"+projectSequence
									+",'"+createdBy
									+"','"+updatedBy
									+"')";
							System.out.println(insertRecords);
							try {
								statement2.executeQuery(insertRecords);
							}catch(SQLException e) {
								result=e.getMessage();
							}
						}
						result="Records inserted in the main table successfully";

					}catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						//TODO: Log the error message
						
						result=e.getMessage();
					}finally {
						try {
							conn.close();
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					return result;	
		}

	


}
