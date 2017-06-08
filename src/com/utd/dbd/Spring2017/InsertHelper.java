package com.utd.dbd.Spring2017;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;

public class InsertHelper {

	private static InsertHelper instance;

	private InsertHelper(){}

	public static InsertHelper getInstance(){
		if(instance == null)
			return new InsertHelper();
		return instance;
	}
	
	/**
	 * @return the rowCount
	 */
	public static int getRowCount() {
		return rowCount;
	}

	
	public static void setRowCount(int rowCount) {
		InsertHelper.rowCount = rowCount;
	}


	private TreeMap<Integer,List<String>> rowMapper = new TreeMap<Integer,List<String>>();;
	private TreeMap<String,TreeMap<Integer,List<String>>> tableDataMapper = new TreeMap<String,TreeMap<Integer,List<String>>>();
	public static TreeMap<String,TreeMap<String,TreeMap<Integer,List<String>>>> tableSchemaDataMapper = new TreeMap<String,TreeMap<String,TreeMap<Integer,List<String>>>>();
	private static TreeMap<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>> tableIndexDataMapper = new TreeMap<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>>();
	private static int rowCount = 0;

	
	public boolean insertValues(
			String schemaName,
			String tableName,
			String[] columnValues,
			TreeMap<Integer,List<String>> columnSchema){

		boolean updateSuccess = true;
		Set<Map.Entry<Integer,List<String>>> columnSet = columnSchema.entrySet();
		Iterator<Map.Entry<Integer,List<String>>> columnItr = columnSet.iterator();
		List<String> columnData = new ArrayList<String>();

		while(columnItr.hasNext()){

			Map.Entry<Integer,List<String>> columnME = columnItr.next();
			int position = columnME.getKey();
			List<String> currentColumn = columnME.getValue();
			try {
			
			if(currentColumn.get(4).equals("YES")){
				if(checkPKConstraint(schemaName,tableName,position,columnValues[position - 1])){
					System.out.println("Primary Key Constraint Violation....");
					updateSuccess = false;
					break;
				} else if (columnValues[position - 1].equalsIgnoreCase("NULL") || columnValues[position - 1] == ""){
					System.out.println("Primary Key Can't be Null....");
					updateSuccess = false;
					break;
				} else if(currentColumn.get(1).equals("DATETIME")){ 
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else if(currentColumn.get(1).equals("DATE")){ 
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else {
					columnData.add(columnValues[position - 1]);
				}
			} else if(currentColumn.get(3).equals("YES")){ 
				if(columnValues[position - 1].equalsIgnoreCase("NULL") || columnValues[position - 1] == ""){
					System.out.println("Violates NULL Constraint...");
					updateSuccess = false;
					break;
				} else if(currentColumn.get(1).equals("DATETIME")){ 
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else if(currentColumn.get(1).equals("DATE")){ 
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else {
					columnData.add(columnValues[position - 1]);
				}
			} else {
				if(currentColumn.get(1).equals("DATETIME")){ 
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else if(currentColumn.get(1).equals("DATE")){ 
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else {
					columnData.add(columnValues[position - 1]);
				}
			}	
					
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		if(updateSuccess){
			rowCount = getRowCount(schemaName,tableName);

			InsertHelper.setRowCount(rowCount + 1);

			rowMapper = updateRowMap(schemaName,tableName);
			rowMapper.put(rowCount, columnData);

			tableDataMapper.put(tableName, rowMapper);

			InsertHelper.tableSchemaDataMapper.put(schemaName,tableDataMapper);
		}
		return updateSuccess;
	}

	
	public boolean updateValues(
			String schemaName,
			String tableName,
			String[] columnValues,
			TreeMap<Integer,List<String>> columnSchema){

		boolean updateSuccess = true;
		Set<Map.Entry<Integer,List<String>>> columnSet = columnSchema.entrySet();
		Iterator<Map.Entry<Integer,List<String>>> columnItr = columnSet.iterator();
		List<String> columnData = new ArrayList<String>();

		while(columnItr.hasNext()){

			Map.Entry<Integer,List<String>> columnME = columnItr.next();
			int position = columnME.getKey();
			List<String> currentColumn = columnME.getValue();
			try {
			 
			if(currentColumn.get(4).equals("YES")){
				if(checkPKConstraint(schemaName,tableName,position,columnValues[position - 1])){
					System.out.println("Primary key value cannot be changed!");
					updateSuccess = false;
					break;
				} else if (columnValues[position - 1].equalsIgnoreCase("NULL") || columnValues[position - 1] == ""){
					System.out.println("Primary Key Can't be Null!");
					updateSuccess = false;
					break;
				} else if(currentColumn.get(1).equals("DATETIME")){
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else if(currentColumn.get(1).equals("DATE")){ 
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else {
					columnData.add(columnValues[position - 1]);
				}
			} else if(currentColumn.get(3).equals("YES")){ 
				if(columnValues[position - 1].equalsIgnoreCase("NULL") || columnValues[position - 1] == ""){
					System.out.println("Violates NULL Constraint!");
					updateSuccess = false;
					break;
				} else if(currentColumn.get(1).equals("DATETIME")){ 
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else if(currentColumn.get(1).equals("DATE")){ 
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else {
					columnData.add(columnValues[position - 1]);
				}
			} else {
				if(currentColumn.get(1).equals("DATETIME")){
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else if(currentColumn.get(1).equals("DATE")){ 
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
					Date date = dateFormat.parse(columnValues[position - 1]);
					String parsedDate = dateFormat.format(date);
					columnData.add(parsedDate);
				} else {
					columnData.add(columnValues[position - 1]);
				}
			}	
					
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		if(updateSuccess){
			rowCount = getRowCount(schemaName,tableName);

			InsertHelper.setRowCount(rowCount + 1);

			rowMapper = updateRowMap(schemaName,tableName);
			rowMapper.put(rowCount, columnData);

			tableDataMapper.put(tableName, rowMapper);

			InsertHelper.tableSchemaDataMapper.put(schemaName,tableDataMapper);
		}
		return updateSuccess;
	}

	
	
	public TreeMap<Integer,List<String>> updateRowMap(String schemaName,String tableName){
		TreeMap<Integer,List<String>> updatedRowMap = new TreeMap<Integer,List<String>>();
		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaDataMapper.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaItr = tableSchemaSet.iterator();

		while(tableSchemaItr.hasNext()){
			Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>> me =  tableSchemaItr.next();
			String schema = me.getKey();

			if(schema.equals(schemaName)){
				TreeMap<String,TreeMap<Integer,List<String>>> currentTable = me.getValue();
				Set<Map.Entry<String,TreeMap<Integer,List<String>>>> tableSet = currentTable.entrySet();
				Iterator<Map.Entry<String,TreeMap<Integer,List<String>>>> tableItr = tableSet.iterator();

				while(tableItr.hasNext()){
					Map.Entry<String, TreeMap<Integer,List<String>>> tableME = tableItr.next();
					String currentTableName = tableME.getKey();
					if(currentTableName.equals(tableName)){
						updatedRowMap = tableME.getValue();	
					}
				}
			}
		}		
		return updatedRowMap;
	} 

	
	public boolean checkPKConstraint(String schemaName,String tableName,int position,String col){
		boolean isConstraintViolated = false;
		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaDataMapper.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaItr = tableSchemaSet.iterator();

		while(tableSchemaItr.hasNext()){
			Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>> me =  tableSchemaItr.next();
			String schema = me.getKey();

			if(schema.equals(schemaName)){
				TreeMap<String,TreeMap<Integer,List<String>>> currentTable = me.getValue();
				Set<Map.Entry<String,TreeMap<Integer,List<String>>>> tableSet = currentTable.entrySet();
				Iterator<Map.Entry<String,TreeMap<Integer,List<String>>>> tableItr = tableSet.iterator();

				while(tableItr.hasNext()){
					Map.Entry<String, TreeMap<Integer,List<String>>> tableME = tableItr.next();
					String currentTableName = tableME.getKey();
					if(currentTableName.equals(tableName)){
						TreeMap<Integer,List<String>> ordinal = tableME.getValue();
						Set<Map.Entry<Integer,List<String>>> ordinalSet = ordinal.entrySet();
						Iterator<Map.Entry<Integer,List<String>>> ordinalItr = ordinalSet.iterator();

						while(ordinalItr.hasNext()){
							Map.Entry<Integer,List<String>> columnME = ordinalItr.next();
							List<String> row = columnME.getValue();
							String rowValue = row.get(position - 1);
							if(rowValue.equals(col)){
								isConstraintViolated = true;
								break;
							}
						}

					}
				}
			}
		}
		return isConstraintViolated;
	}

	public int getRowCount(String schemaName,String tableName){
		int rowCount = 0;
		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaDataMapper.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaItr = tableSchemaSet.iterator();

		while(tableSchemaItr.hasNext()){
			Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>> me =  tableSchemaItr.next();
			String schema = me.getKey();

			if(schema.equals(schemaName)){
				TreeMap<String,TreeMap<Integer,List<String>>> currentTable = me.getValue();
				Set<Map.Entry<String,TreeMap<Integer,List<String>>>> tableSet = currentTable.entrySet();
				Iterator<Map.Entry<String,TreeMap<Integer,List<String>>>> tableItr = tableSet.iterator();

				while(tableItr.hasNext()){
					Map.Entry<String, TreeMap<Integer,List<String>>> tableME = tableItr.next();
					String currentTableName = tableME.getKey();
					if(currentTableName.equals(tableName)){
						TreeMap<Integer,List<String>> ordinal = tableME.getValue();
						Set<Map.Entry<Integer,List<String>>> ordinalSet = ordinal.entrySet();
						Iterator<Map.Entry<Integer,List<String>>> ordinalItr = ordinalSet.iterator();

						while(ordinalItr.hasNext()){
							Map.Entry<Integer,List<String>> columnME = ordinalItr.next();
							rowCount = columnME.getKey();
						}

					}
				}
			}
		}
		return rowCount;
	} 


	public static TreeMap<Integer,List<String>> getTableData(String schemaName,String tableName){
		TreeMap<Integer,List<String>> tableDataMap = new TreeMap<Integer,List<String>>();

		Set<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaSet = tableSchemaDataMapper.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>>> tableSchemaItr = tableSchemaSet.iterator();

		while(tableSchemaItr.hasNext()){
			Map.Entry<String,TreeMap<String,TreeMap<Integer,List<String>>>> me =  tableSchemaItr.next();
			String schema = me.getKey();

			if(schema.equals(schemaName)){
				TreeMap<String,TreeMap<Integer,List<String>>> currentTable = me.getValue();
				Set<Map.Entry<String,TreeMap<Integer,List<String>>>> tableSet = currentTable.entrySet();
				Iterator<Map.Entry<String,TreeMap<Integer,List<String>>>> tableItr = tableSet.iterator();

				while(tableItr.hasNext()){
					Map.Entry<String, TreeMap<Integer,List<String>>> tableME = tableItr.next();
					String currentTableName = tableME.getKey();
					if(currentTableName.equals(tableName)){
						tableDataMap = tableME.getValue();
					}
				}
			}
		}

		return tableDataMap;
	} 

	
	public static TreeMap<String,TreeMap<String,List<String>>> getIndexData(String schemaName,String tableName){
		TreeMap<String,TreeMap<String,List<String>>> tableIndexMap = new TreeMap<String,TreeMap<String,List<String>>>();

		Set<Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>>> tableIndexDataSet = tableIndexDataMapper.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>>> tableIndexItr = tableIndexDataSet.iterator();

		while(tableIndexItr.hasNext()){
			Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>> me = tableIndexItr.next();
			String currentSchemaName = me.getKey();
			if(currentSchemaName.equals(schemaName)){
				TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>> tableIndex = me.getValue();
				Set<Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>>> tableIndexSet = tableIndex.entrySet();
				Iterator<Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>>> tableItr = tableIndexSet.iterator();

				while(tableItr.hasNext()){
					Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>> tableME = tableItr.next();
					String currentTableName = tableME.getKey();
					if(currentTableName.equals(tableName)){
						tableIndexMap = tableME.getValue();
					}
				}
			}
		}
		return tableIndexMap;
	} 

	
	public void updateIndex(String schemaName,String tableName,String columnName,long pointerValue, String type,String key){
		TreeMap<String,TreeMap<String,List<String>>> tableIndexStream = new TreeMap<String,TreeMap<String,List<String>>>();

		List<String> indexValue = new ArrayList<String>();
		int pointerCounter = 0;

		TreeMap<String,List<String>> idx = new TreeMap<String,List<String>>();
		TreeMap<String,TreeMap<String,List<String>>> columnIndexMap = new TreeMap<String,TreeMap<String,List<String>>>();
		TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>> tableIndexMap = new TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>();  

		tableIndexStream = getCurrentIndexStream(schemaName,tableName,columnName);

		if(!tableIndexStream.isEmpty()){
			Set<Map.Entry<String,TreeMap<String, List<String>>>> tableIndexStreamSet = tableIndexStream.entrySet();
			Iterator<Map.Entry<String,TreeMap<String, List<String>>>> tableIndexStreamItr = tableIndexStreamSet.iterator();

			while(tableIndexStreamItr.hasNext()){
				Map.Entry<String,TreeMap<String,List<String>>> indexStreamME = tableIndexStreamItr.next();
				String existingColumn = indexStreamME.getKey();
				TreeMap<String,List<String>> indexStreamValue = indexStreamME.getValue();

				Set<Map.Entry<String, List<String>>> indexStreamSet = indexStreamValue.entrySet();
				Iterator<Map.Entry<String, List<String>>> indexStreamItr = indexStreamSet.iterator();

				while(indexStreamItr.hasNext()){
					Map.Entry<String, List<String>> currentME = indexStreamItr.next();
					String currentK = currentME.getKey();
					List<String> currentV = currentME.getValue();

					
					columnIndexMap.put(existingColumn,indexStreamValue);

					if(!currentV.isEmpty()){
						if(currentK.equals(key)){
							indexValue.add(currentV.get(0));
							
							int currentPointerCounter = Integer.parseInt(currentV.get(1));
							pointerCounter = currentPointerCounter + 1;
							indexValue.add(Integer.toString(pointerCounter));

							for(int i = 0; i < currentPointerCounter; i++)
								indexValue.add(currentV.get(i + 2));

							indexValue.add(Long.toString(pointerValue));
						} else {
							pointerCounter = pointerCounter + 1;
							indexValue.add(type);
							indexValue.add(Integer.toString(pointerCounter));
							indexValue.add(Long.toString(pointerValue));
						}
					} else {
						pointerCounter = pointerCounter + 1;
						indexValue.add(type);
						indexValue.add(Integer.toString(pointerCounter));
						indexValue.add(Long.toString(pointerValue));
					}
				}
			}
		} else {
			pointerCounter = pointerCounter + 1;
			indexValue.add(type);
			indexValue.add(Integer.toString(pointerCounter));
			indexValue.add(Long.toString(pointerValue));
		}

		idx.put(key, indexValue);
		columnIndexMap.put(columnName,idx);
		tableIndexMap.put(tableName,columnIndexMap);
		InsertHelper.tableIndexDataMapper.put(schemaName, tableIndexMap);
	}

	
	public TreeMap<String,TreeMap<String,List<String>>> getCurrentIndexStream(String schemaName,String tableName,String columnName){
		TreeMap<String,TreeMap<String,List<String>>> indexStream = new TreeMap<String,TreeMap<String,List<String>>>();

		Set<Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>>> tableIndexDataSet = tableIndexDataMapper.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>>> tableIndexItr = tableIndexDataSet.iterator();

		
		while(tableIndexItr.hasNext()){
			Map.Entry<String,TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>>> me = tableIndexItr.next();
			String currentSchemaName = me.getKey();
			if(currentSchemaName.equals(schemaName)){
				TreeMap<String,TreeMap<String,TreeMap<String,List<String>>>> tableIndex = me.getValue();
				Set<Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>>> tableIndexSet = tableIndex.entrySet();
				Iterator<Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>>> tableItr = tableIndexSet.iterator();

				
				while(tableItr.hasNext()){
					Map.Entry<String,TreeMap<String,TreeMap<String,List<String>>>> tableME = tableItr.next();
					String currentTableName = tableME.getKey();
					if(currentTableName.equals(tableName)){
						indexStream = tableME.getValue();
					}
				}
			}
		}
		return indexStream;
	}

	}
