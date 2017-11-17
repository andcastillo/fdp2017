package org.db.core;

import java.util.List;

public class Node {

	private String type;
	private List<String> tableInput;
	private List<String> parameters;
	private String tableNameOutput;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public List<String> getTableInput() {
		return tableInput;
	}
	public void setTableInput(List<String> tablesInput) {
		this.tableInput = tablesInput;
	}
	public List<String> getParameters() {
		return parameters;
	}
	public void setParameters(List<String> parameters) {
		this.parameters = parameters;
	}
	
	public String getTableNameOutput() {
		return tableNameOutput;
	}
	public void setTableNameOutput(String tableNameOutput) {
		this.tableNameOutput = tableNameOutput;
	}
	
	
}
