package com.infosys.juniper.dto;

public class TempTableMetadataDto {

	String table_name;
	String columns;
	String where_clause;
	String fetch_type;
	String incr_col;
	String view_flag;
	String view_source_schema;
	String validation_flag;
	String error_message;
	
	
	
	
	
	public String getValidation_flag() {
		return validation_flag;
	}
	public void setValidation_flag(String validation_flag) {
		this.validation_flag = validation_flag;
	}
	public String getError_message() {
		return error_message;
	}
	public void setError_message(String error_message) {
		this.error_message = error_message;
	}
	public String getView_flag() {
		return view_flag;
	}
	public void setView_flag(String view_flag) {
		this.view_flag = view_flag;
	}
	public String getView_source_schema() {
		return view_source_schema;
	}
	public void setView_source_schema(String view_source_schema) {
		this.view_source_schema = view_source_schema;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public String getColumns() {
		return columns;
	}
	public void setColumns(String columns) {
		this.columns = columns;
	}
	public String getWhere_clause() {
		return where_clause;
	}
	public void setWhere_clause(String where_clause) {
		this.where_clause = where_clause;
	}
	public String getFetch_type() {
		return fetch_type;
	}
	public void setFetch_type(String fetch_type) {
		this.fetch_type = fetch_type;
	}
	public String getIncr_col() {
		return incr_col;
	}
	public void setIncr_col(String incr_col) {
		this.incr_col = incr_col;
	}
	
	
}
