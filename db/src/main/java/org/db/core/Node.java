package org.db.core;

import java.util.List;

public class Node {

	private String operationName;
	private List<String> tableInput;
	private List<String> parameters;
	private String tableNameOutput;
	private String scanMethod = "seq";
	
	public Node(String operationName, List<String> tableInput, List<String> parameters, 
			String tableNameOutput, String scanMethod) {
		this.operationName = operationName;
		this.tableInput = tableInput;
		this.parameters = parameters;
		this.tableNameOutput = tableNameOutput;
		if(scanMethod != null)
			this.scanMethod = scanMethod;
	}
	
	
	public Node() {
		
	}
	
	public String getOperationName() {
		return this.operationName;
	}
	public void setOperationName(String operationName) {
		this.operationName = operationName;
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
	public String getScanMethod() {
		return scanMethod;
	}
	public void setScanMethod(String scanMethod) {
		this.scanMethod = scanMethod;
	}
	
	
}
