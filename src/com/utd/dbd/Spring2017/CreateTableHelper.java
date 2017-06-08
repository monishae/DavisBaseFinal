package com.utd.dbd.Spring2017;

public class CreateTableHelper{

	private String colName = null;
	private String dataType = null;
	private boolean isPK = false;
	private boolean isNull = false;

	
	public String getColumnName() {
		return colName;
	}
	public void setColumnName(String columnName) {
		this.colName = columnName;
	}
	
	
	public String getDataType() {
		return dataType;
	}
	
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	
	public boolean isPK() {
		return isPK;
	}
	
 
	public void setPK(boolean isPrimaryKey) {
		this.isPK = isPrimaryKey;
	}
	
	public boolean isNull() {
		return isNull;
	}
	
	
	public void setNull(boolean isNull) {
		this.isNull = isNull;
	}
}
