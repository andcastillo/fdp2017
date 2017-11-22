package org.db.core;

public class Schema {

	private String path;
	private int length;
	private Attribute[] attribute;
	
	public Schema(String path, int length) {
		super();
		this.path = path;
		this.length = length;
		this.setAttribute(new Attribute[length]);
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	public Attribute[] getAttribute() {
		return attribute;
	}
	public void setAttribute(Attribute[] attribute) {
		this.attribute = attribute;
	}
	public void addAttribute(Attribute attribute, int index) {
		this.attribute[index] = attribute;
	}
}
