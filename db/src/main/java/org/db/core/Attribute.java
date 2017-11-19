package org.db.core;

public class Attribute {
	
	int index;
	Object scan;
	String columnName;
	String type;

	public Attribute(int index, Object scan, String columnName) {
		super();
		this.index = index;
		this.scan = scan;
		this.columnName = columnName;
	}
	public Attribute(int index, Object scan, String columnName, String type) {
		super();
		this.index = index;
		this.scan = scan;
		this.columnName = columnName;
		this.type = type;
	}
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public Object getScan() {
		return scan;
	}
	public void setScan(Object scan) {
		this.scan = scan;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
}
