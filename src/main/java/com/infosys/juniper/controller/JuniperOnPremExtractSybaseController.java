package com.infosys.juniper.controller;

import java.sql.SQLException;

import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.infosys.juniper.dto.ConnectionDto;
import com.infosys.juniper.dto.RequestDto;
import com.infosys.juniper.dto.TempTableInfoDto;
import com.infosys.juniper.dto.TempTableMetadataDto;
import com.infosys.juniper.repository.JuniperOnPremExtractSybaseRepository;
import com.infosys.juniper.util.ResponseUtil;


@CrossOrigin
@RestController
public class JuniperOnPremExtractSybaseController {

	@Autowired
	JuniperOnPremExtractSybaseRepository repository;
	
	
	@RequestMapping(value = "/addSybaseConnection", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addSybaseConnection(@RequestBody RequestDto requestDto) throws Exception {
		// Parse json to Dto Object
		String testConnStatus="success";
		String response;
		String status = "";
		String message = "";
		ConnectionDto connDto = new ConnectionDto();
		connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
		connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
		connDto.setHostName(requestDto.getBody().get("data").get("host_name"));
		connDto.setPort(requestDto.getBody().get("data").get("port"));
		connDto.setUserName(requestDto.getBody().get("data").get("user_name"));
		connDto.setPassword(requestDto.getBody().get("data").get("password"));
		connDto.setDbName(requestDto.getBody().get("data").get("db_name"));
		//connDto.setServiceName(requestDto.getBody().get("data").get("service_name"));
		connDto.setSystem(requestDto.getBody().get("data").get("system"));
		connDto.setProject(requestDto.getBody().get("data").get("project"));
		connDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
		System.out.println("Testing Sybase Connection");
		
		testConnStatus=repository.testSybaseConnection(connDto);
		
		if(testConnStatus.equalsIgnoreCase("SUCCESS")) {
			response = repository.addSybaseConnectionDetails(connDto);
				if(response.toLowerCase().contains("success")) {
					status="Success";
					message="Connection created with Connection Id "+response.split(":")[1];
				}
				else {
					status="Failed";
					message=response;
				}

			}
		else {
			status="Failed";
			message="Could not establish connection to the source database";
			}
			
		return ResponseUtil.createResponse(status, message);	

	}	
	
	@RequestMapping(value = "/updSybaseConnection", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String updConnection(@RequestBody RequestDto requestDto) throws Exception {
		// Parse json to Dto Object
		String testConnStatus="";
		String status = "";
		String message = "";
		String response="";
		ConnectionDto connDto = new ConnectionDto();


		connDto.setConnId(Integer.parseInt(requestDto.getBody().get("data").get("conn")));
		connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
		connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
		connDto.setSystem(requestDto.getBody().get("data").get("system"));
		connDto.setHostName(requestDto.getBody().get("data").get("host_name"));
		connDto.setPort(requestDto.getBody().get("data").get("port"));
		connDto.setUserName(requestDto.getBody().get("data").get("user_name"));
		connDto.setPassword(requestDto.getBody().get("data").get("password"));
		connDto.setDbName(requestDto.getBody().get("data").get("db_name"));
//		connDto.setServiceName(requestDto.getBody().get("data").get("service_name"));
		connDto.setProject(requestDto.getBody().get("data").get("project"));
		connDto.setJuniper_user(requestDto.getBody().get("data").get("user"));

		testConnStatus=repository.testSybaseConnection(connDto);
		if(testConnStatus.equalsIgnoreCase("SUCCESS")) {
			response = repository.updateOracleConnectionDetails(connDto);
			if(response.toLowerCase().contains("success")){
				status="Success";
				message="Details updated Successfully";
			}
			else {
				status="Failed";
				message=response;
			}
		}
		else {
			status="Failed";
			message="Failed while connecting to the source database";
		}


		return ResponseUtil.createResponse(status, message);
	}
	
	@PostMapping(value="/delSybaseConnection", consumes="application/json")
	public String deleteSybaseConnection(@RequestBody RequestDto requestDto) throws SQLException{
		String status="";
		String message="";
		String response="";
		ConnectionDto connDto = new ConnectionDto();
		String testConnStatus="";
		
		connDto.setConnId(Integer.parseInt(requestDto.getBody().get("data").get("conn")));
		connDto.setConn_name(requestDto.getBody().get("data").get("connection_name"));
		connDto.setConn_type(requestDto.getBody().get("data").get("connection_type"));
		connDto.setSystem(requestDto.getBody().get("data").get("system"));
		connDto.setHostName(requestDto.getBody().get("data").get("host_name"));
		connDto.setPort(requestDto.getBody().get("data").get("port"));
		connDto.setUserName(requestDto.getBody().get("data").get("user_name"));
		connDto.setPassword(requestDto.getBody().get("data").get("password"));
		connDto.setDbName(requestDto.getBody().get("data").get("db_name"));
//		connDto.setServiceName(requestDto.getBody().get("data").get("service_name"));
		connDto.setProject(requestDto.getBody().get("data").get("project"));
		connDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
		
		testConnStatus=repository.testSybaseConnection(connDto);
		if(testConnStatus.equalsIgnoreCase("SUCCESS")) {
			response = repository.deleteSybaseConnectionDetails(connDto);
			if(response.toLowerCase().contains("success")){
				status="Success";
				message="Details Deleted Successfully";
			}
			else {
				status="Failed";
				message=response;
			}
		}
		else {
			status="Failed";
			message="Failed while connecting to the source database";
		}


		return ResponseUtil.createResponse(status, message);
		
	}
	
	
	@RequestMapping(value = "/addTempTableInfo", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String addTempTableInfo(@RequestBody RequestDto requestDto) throws SQLException {
		String status="";
		String message="";
		String response="";
		
		//String load_type=requestDto.getBody().get("data").get("load_type");
		String feed_id=requestDto.getBody().get("data").get("feed_id");
		String src_type=requestDto.getBody().get("data").get("src_type");
		
		response=repository.editTempTableDetails(feed_id,src_type);
	
		if(response.toLowerCase().contains("success")) {
			TempTableInfoDto tempTableInfoDto=new TempTableInfoDto();

			ArrayList<TempTableMetadataDto> tempTableMetadataArr=new ArrayList<TempTableMetadataDto>();
			int counter=Integer.parseInt(requestDto.getBody().get("data").get("counter"));
			String load_type=requestDto.getBody().get("data").get("load_type");
			
			
			for(int i=1;i<=counter;i++) {
				TempTableMetadataDto tableMetadata=new TempTableMetadataDto();
				
				tableMetadata.setTable_name(requestDto.getBody().get("data").get("table_name"+i).toUpperCase());
				String view_flag=requestDto.getBody().get("data").get("view_flag"+i);
				if(view_flag==null|| view_flag.isEmpty()) {
					tableMetadata.setView_flag("N");
				}
				else {
					tableMetadata.setView_flag(view_flag);
					tableMetadata.setView_source_schema(requestDto.getBody().get("data").get("view_src_schema"+i));
				}
				
				
				tableMetadata.setColumns(requestDto.getBody().get("data").get("columns_name"+i).toUpperCase());
				tableMetadata.setWhere_clause(requestDto.getBody().get("data").get("where_clause"+i));
				tableMetadata.setFetch_type(requestDto.getBody().get("data").get("fetch_type"+i));
				tableMetadata.setIncr_col(requestDto.getBody().get("data").get("incr_col"+i));
				tempTableMetadataArr.add(tableMetadata);

			}
			
			if(load_type ==null || load_type.isEmpty()) {
				tempTableInfoDto.setLoad_type("ind");
			}
			else {
				tempTableInfoDto.setLoad_type(load_type);
			}
			
			tempTableInfoDto.setTableMetadataArr(tempTableMetadataArr);
			tempTableInfoDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
			tempTableInfoDto.setProject(requestDto.getBody().get("data").get("project"));
			tempTableInfoDto.setFeed_id(Integer.parseInt(requestDto.getBody().get("data").get("feed_id")));
			response=repository.addTempTableDetails(tempTableInfoDto);
			if(response.toLowerCase().contains("success")) {
				status="Success";
				message="Table Details Added Successfully";
			}
			else {
				status="Failed";
				message=response;
			}
		}else {
			status="Failed";
			message=response;
		}
		return ResponseUtil.createResponse(status, message);
	}
	
	@RequestMapping(value = "/editTempTableInfo", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String editTempTableInfo(@RequestBody RequestDto requestDto) throws SQLException {
		String status="";
		String message="";
		String response="";
		
		//String load_type=requestDto.getBody().get("data").get("load_type");
		String feedId=requestDto.getBody().get("data").get("feed_id");
		String srcType=requestDto.getBody().get("data").get("src_type");
		
		response=repository.editTempTableDetails(feedId,srcType);
	
		if(response.toLowerCase().contains("success")) {
			TempTableInfoDto tempTableInfoDto=new TempTableInfoDto();

			ArrayList<TempTableMetadataDto> tempTableMetadataArr=new ArrayList<TempTableMetadataDto>();
			int counter=Integer.parseInt(requestDto.getBody().get("data").get("counter"));
			String load_type=requestDto.getBody().get("data").get("load_type");
			
			
			for(int i=1;i<=counter;i++) {
				TempTableMetadataDto tableMetadata=new TempTableMetadataDto();
				
				tableMetadata.setTable_name(requestDto.getBody().get("data").get("table_name"+i).toUpperCase());
				String viewFlag=requestDto.getBody().get("data").get("view_flag"+i);
				if(viewFlag==null|| viewFlag.isEmpty()) {
					tableMetadata.setView_flag("N");
				}
				else {
					tableMetadata.setView_flag(viewFlag);
					tableMetadata.setView_source_schema(requestDto.getBody().get("data").get("view_src_schema"+i));
				}
				
				
				tableMetadata.setColumns(requestDto.getBody().get("data").get("columns_name"+i).toUpperCase());
				tableMetadata.setWhere_clause(requestDto.getBody().get("data").get("where_clause"+i));
				tableMetadata.setFetch_type(requestDto.getBody().get("data").get("fetch_type"+i));
				tableMetadata.setIncr_col(requestDto.getBody().get("data").get("incr_col"+i));
				tempTableMetadataArr.add(tableMetadata);

			}
			
			if(load_type ==null || load_type.isEmpty()) {
				tempTableInfoDto.setLoad_type("ind");
			}
			else {
				tempTableInfoDto.setLoad_type(load_type);
			}
			
			tempTableInfoDto.setTableMetadataArr(tempTableMetadataArr);
			tempTableInfoDto.setJuniper_user(requestDto.getBody().get("data").get("user"));
			tempTableInfoDto.setProject(requestDto.getBody().get("data").get("project"));
			tempTableInfoDto.setFeed_id(Integer.parseInt(requestDto.getBody().get("data").get("feed_id")));
			response=repository.addTempTableDetails(tempTableInfoDto);
			if(response.toLowerCase().contains("success")) {
				status="Success";
				message="Table Details Added Successfully. Table IDs are "+response.split(":")[1];
			}
			else {
				status="Failed";
				message=response;
			}
		}else {
			status="Failed";
			message=response;
		}
		return ResponseUtil.createResponse(status, message);
	}
	
	@RequestMapping(value = "/metaDataValidation", method = RequestMethod.POST, consumes = "application/json")
	@ResponseBody
	public String metaDataValidation(@RequestBody RequestDto requestDto) throws SQLException {
		System.out.println("Reached inside Meta data validation block");
		String status="";
		String message="";
		String response="";
		System.out.println(requestDto.toString());
		String feedSequence=requestDto.getBody().get("data").get("feed_sequence");
		String projectId=requestDto.getBody().get("data").get("project_id");
		
		System.out.println("feed_sequence is "+feedSequence);	
		System.out.println("project_id is "+projectId);
		
		response=repository.metaDataValidate(feedSequence,projectId);
		if(response.contains("success")) {			
			status="success";
			message="Metadata validated Successfully";
		}
		else {
			status="Failed";
			message=response;
		}		
		return ResponseUtil.createResponse(status, message);
	}

	

}
