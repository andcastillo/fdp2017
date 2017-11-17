package org.db.core;

import java.util.HashMap;

public class Schema {

	private String path;
	private HashMap<String, Object> indexes;
	private HashMap<String, Object> types;
	
	
	public Schema(String path) {
		super();
		this.path = path;
		this.indexes = new HashMap<String, Object>();
		this.types = new HashMap<String, Object>() ;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	
	public HashMap<String, Object> getIndexes() {
		return indexes;
	}
	public void setIndexes(HashMap<String, Object> indexes) {
		this.indexes = indexes;
	}
	
	public HashMap<String, Object> getTypes() {
		return types;
	}
	public void setTypes(HashMap<String, Object> types) {
		this.types = types;
	}
	
	
	
}
