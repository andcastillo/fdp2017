package org.db.core;

import java.util.ArrayList;
import java.util.List;

public class Node {

	private String operationName;
	private List<Object> tableInput;
	private List<String> parameters;
	private String tableNameOutput;
	private String scanMethod = "seq";
	
	public Node(String operationName, List<Object> tableInput, List<String> parameters, 
			String tableNameOutput, String scanMethod) {
		this.operationName = operationName;
		this.tableInput = tableInput;
		this.parameters = parameters;
		this.tableNameOutput = tableNameOutput;
		if(scanMethod != null)
			this.scanMethod = scanMethod;
	}
	
	
	public Node() {
		this.tableInput = new ArrayList<Object>();
		this.parameters = new ArrayList<String>();
	}
	
	public String getOperationName() {
		return this.operationName;
	}
	public void setOperationName(String operationName) {
		this.operationName = operationName;
	}
	public List<Object> getTableInput() {
		return tableInput;
	}
	public void addTableInput(Object tablesInput) {
		this.tableInput.add(tablesInput);
	}
	public List<String> getParameters() {
		return parameters;
	}
	public void addParameters(String parameters) {
		this.parameters.add(parameters);
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
