package com.infosys.juniper.dto;

import java.util.ArrayList;
import java.util.Map;

public class TempTableInfoDto {
	
	String juniper_user;
	int feed_id;
	ArrayList<TempTableMetadataDto> tempTableMetadataArr;
	String project;
	String incr_flag;
	String load_type;
	
	
	
	public String getLoad_type() {
		return load_type;
	}
	public void setLoad_type(String load_type) {
		this.load_type = load_type;
	}
	public String getIncr_flag() {
		return incr_flag;
	}
	public void setIncr_flag(String incr_flag) {
		this.incr_flag = incr_flag;
	}
	public String getProject() {
		return project;
	}
	public void setProject(String project) {
		this.project = project;
	}
	public String getJuniper_user() {
		return juniper_user;
	}
	public void setJuniper_user(String juniper_user) {
		this.juniper_user = juniper_user;
	}
	public int getFeed_id() {
		return feed_id;
	}
	public void setFeed_id(int feed_id) {
		this.feed_id = feed_id;
	}
	public ArrayList<TempTableMetadataDto> getTempTableMetadataArr() {
		return tempTableMetadataArr;
	}
	public void setTableMetadataArr(ArrayList<TempTableMetadataDto> tempTableMetadataArr) {
		this.tempTableMetadataArr = tempTableMetadataArr;
	}
	
	

}
