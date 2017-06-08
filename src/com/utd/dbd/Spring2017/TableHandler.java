package com.utd.dbd.Spring2017;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
/**
 * 
 * @author monishaelumalai
 *
 */
public class TableHandler {

	private static String tblName = null;
	private static int noOfCols = 0;


	public boolean createTable(String input) throws IOException{
		String orgQuery = input;
		orgQuery = orgQuery.trim();
		String query = input.toLowerCase().trim();
		boolean queryFailed = false;
		
		SchemaHandler currentSchema =  SchemaHandler.getInstance();
		String currentSchemaName = currentSchema.getCurrentSchema();

		
		TableSchemaManager currentTableSchemaManager =  TableSchemaManager.getInstance();
		tblName = orgQuery.substring(query.indexOf("table ")+6,query.indexOf("(")).trim();

		
		if(updateInformationSchemaTable()){
			queryFailed = true;
			return queryFailed;
		}

		String tableContentsWithQuotes = orgQuery.substring(orgQuery.indexOf("(")+1,orgQuery.length());
		String tableData = tableContentsWithQuotes.replace("))", ")");
		tableData = tableData.trim();

		TableSchemaManager.ordMap = new TreeMap<Integer,List<String>>();
		TableSchemaManager.tableMap = new TreeMap<String,TreeMap<Integer,List<String>>>();
		
		
		String[] createTableData = tableData.split("\\,");  
		noOfCols = createTableData.length;
		CreateTableHelper[] helper = new CreateTableHelper[noOfCols];

		 
		for(int item = 0; item < noOfCols ; item++){
			helper[item] = new CreateTableHelper(); 
			
			createTableData[item] = createTableData[item].trim();
			String columnName = createTableData[item].substring(0, createTableData[item].indexOf(" "));
			helper[item].setColumnName(columnName);

			String primaryKeyConst = "(.*)[pP][rR][iI][mM][aA][rR][yY](.*)";
			if (createTableData[item].matches(primaryKeyConst))
				helper[item].setPK(true);
			else
				helper[item].setPK(false);

			String notNullPat = "(.*)[nN][uU][lL][lL](.*)";
			if (createTableData[item].matches(notNullPat))
				helper[item].setNull(true);
			else
				helper[item].setNull(false);

			String bytePat = "(.*)[bB][yY][tT][eE](.*)";
			if (createTableData[item].matches(bytePat)){
				helper[item].setDataType("BYTE");
			}

			String shortPat = "(.*)[sS][hH][oO][rR][tT](.*)";
			if (createTableData[item].matches(shortPat)){
				helper[item].setDataType("SHORT");
			}

			String intPat = "(.*)[iI][nN][tT](.*)";
			if (createTableData[item].matches(intPat)){
				helper[item].setDataType("INT");
			}

			String longPat = "(.*)[lL][oO][nN][gG](.*)";
			if (createTableData[item].matches(longPat)){
				helper[item].setDataType("LONG");
			}

			String charPat = "(.*)[cC][hH][aA][rR](.*)";
			if (createTableData[item].matches(charPat)){
				String size = createTableData[item].substring(createTableData[item].indexOf("(")+1, createTableData[item].indexOf(")"));
				helper[item].setDataType("CHAR(" + size + ")");
			}
		
			String varcharPat = "(.*)[vV][aA][rR][cC][hH][aA][rR](.*)";
			if (createTableData[item].matches(varcharPat)){
				String size = createTableData[item].substring(createTableData[item].indexOf("(")+1, createTableData[item].indexOf(")"));
				helper[item].setDataType("VARCHAR(" + size + ")");
			}

			String floatPat = "(.*)[fF][lL][oO][aA][tT](.*)";
			if (createTableData[item].matches(floatPat)){
				helper[item].setDataType("FLOAT");				
			}

			String doublePat = "(.*)[dD][oO][uU][bB][lL][eE](.*)";
			if (createTableData[item].matches(doublePat)){
				helper[item].setDataType("DOUBLE");
			}
			
			String dateTimePat = "(.*)[dD][aA][tT][eE][tT][iI][mM][eE](.*)";
			if (createTableData[item].matches(dateTimePat)){
				helper[item].setDataType("DATETIME");
			}
			String datePat = "(.*)[dD][aA][tT][eE](.*)";
			if (createTableData[item].matches(datePat)){
				helper[item].setDataType("DATE");
			}

			currentTableSchemaManager.newTableSchema(
					tblName,
					item,
					helper[item].getColumnName(),
					helper[item].getDataType(),
					helper[item].isNull(),
					helper[item].isPK()
					);

	
			updateInformationSchemaColumn(
					helper[item].getColumnName(),
					item,
					helper[item].getDataType(),
					helper[item].isNull(),
					helper[item].isPK()
					);

			String newTableIndexName = currentSchemaName + "." + tblName + "." +helper[item].getColumnName()+ ".tbl.ndx";
			RandomAccessFile newTableIndexFile = new RandomAccessFile(newTableIndexName, "rw");
			newTableIndexFile.close();

		}

		TableSchemaManager.tableMap.put(tblName, TableSchemaManager.ordMap);
		currentTableSchemaManager.updateTableSchema(currentSchemaName,tblName);
		

		String newTableDataName = currentSchemaName + "." + tblName + ".tbl";
		RandomAccessFile newTableDataFile = new RandomAccessFile(newTableDataName, "rw");
		newTableDataFile.close();
		return queryFailed;
	}

	
	public boolean updateInformationSchemaTable(){
		SchemaHandler currentSchema =  SchemaHandler.getInstance();
		String currentSchemaName = currentSchema.getCurrentSchema();
		boolean isFound = false;
		try {
			RandomAccessFile tableFile = new RandomAccessFile("information_schema.table.tbl", "rw");

			while(tableFile.getFilePointer() < tableFile.length()){
				String readSchemaName = "";
				byte vcLength = tableFile.readByte();
				for(int j = 0; j < vcLength; j++)
					readSchemaName += (char)tableFile.readByte();

				if(readSchemaName.equals(currentSchemaName)){
					String readTableName = "";
					byte vctLength = tableFile.readByte();
					for(int j = 0; j < vctLength; j++)
						readTableName += (char)tableFile.readByte();

					if(readTableName.equals(tblName)){
						isFound = true;
						System.out.println("Table '" + tblName + "' already exits...");
						break;
					}

					tableFile.readLong();
				} else {
					byte traverseLength = tableFile.readByte();
					for(int j = 0; j < traverseLength; j++)
						tableFile.readByte();
					tableFile.readLong();
				}	
			}

			if(!isFound){
				tableFile.seek(tableFile.length());
				tableFile.writeByte(currentSchemaName.length());
				tableFile.writeBytes(currentSchemaName);
				tableFile.writeByte(tblName.length()); 
				tableFile.writeBytes(tblName);
				tableFile.writeLong(0); 
			}

			tableFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
		return isFound;
	}

	
	public void updateInformationSchemaColumn(
			String columnName,
			int ordPosition,
			String columnType,
			boolean isNull,
			boolean isPrimaryKey
			){
		SchemaHandler currentSchema =  SchemaHandler.getInstance();
		String currentSchemaName = currentSchema.getCurrentSchema();
		try {
			RandomAccessFile columnsFile = new RandomAccessFile("information_schema.columns.tbl", "rw");

				
			columnsFile.seek(columnsFile.length());
			columnsFile.writeByte(currentSchemaName.length());
			columnsFile.writeBytes(currentSchemaName);
			columnsFile.writeByte(tblName.length());
			columnsFile.writeBytes(tblName);
			columnsFile.writeByte(columnName.length());
			columnsFile.writeBytes(columnName);
			columnsFile.writeInt(ordPosition+1); 
			columnsFile.writeByte(columnType.length()); 
			columnsFile.writeBytes(columnType);

			if (isNull) {
				columnsFile.writeByte("YES".length()); 
				columnsFile.writeBytes("YES");
			} else {
				columnsFile.writeByte("NO".length());
				columnsFile.writeBytes("NO");
			}

			if (isPrimaryKey) {
				columnsFile.writeByte("PRI".length());
				columnsFile.writeBytes("PRI");
			} else {
				columnsFile.writeByte("".length());
				columnsFile.writeBytes("");
			}

	
			columnsFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	} 

	
	public void showTables(){
		SchemaHandler schemaTable =  SchemaHandler.getInstance();
		String currentSchemaName = schemaTable.getCurrentSchema();

		try{
			ArrayList<String> tableList = new ArrayList<String>();

			RandomAccessFile tableFile = new RandomAccessFile("information_schema.table.tbl","rw");

			while(tableFile.getFilePointer() < tableFile.length()){
				String readSchemaName = "";
				String readTableName = "";

				byte vcLength = tableFile.readByte();
				for(int j = 0; j < vcLength; j++)
					readSchemaName += (char)tableFile.readByte();

				byte vctLength = tableFile.readByte();
				for(int k = 0; k < vctLength; k++)
					readTableName += (char)tableFile.readByte();
				if(readSchemaName.equals(currentSchemaName)){	
					tableList.add(readTableName);
				}
				tableFile.readLong();
			}

			if(tableList.size() != 0){
				System.out.println(SystemDescription.line("-", 50));
				System.out.println("Table_in_" + currentSchemaName);
				System.out.println(SystemDescription.line("-", 50));
				for(int i = 0; i < tableList.size() ; i++)
					System.out.println(tableList.get(i));
				System.out.println(SystemDescription.line("-", 50));

				tableList.removeAll(tableList);
			} else {
				System.out.println("Empty Set...");
			}

			tableFile.close();
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
	} 
	public boolean isTablePresent(String currentSchemaName, String table){
		boolean isTablePresent = false;
		try{
			RandomAccessFile tableFile = new RandomAccessFile("information_schema.table.tbl","rw");

			while(tableFile.getFilePointer() < tableFile.length()){
				String getSchemaName = "";
				String getTableName = "";

				byte vcLength = tableFile.readByte();
				for(int j = 0; j < vcLength; j++)
					getSchemaName += (char)tableFile.readByte();

				byte vcTableLength = tableFile.readByte();
				for(int k = 0; k < vcTableLength; k++)
					getTableName += (char)tableFile.readByte();

				if(getSchemaName.equals(currentSchemaName)){
					if(getTableName.equals(table)){
						isTablePresent = true;
						break;
					}
				}

				tableFile.readLong();
			}

			if(!isTablePresent)
				System.out.println(currentSchemaName + "." + table +" doesn't exist!");
			tableFile.close();
		}catch(FileNotFoundException e){
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
		return isTablePresent;
	} 

	
	public boolean insertTable(String input){
		TableSchemaManager manager = TableSchemaManager.getInstance();
		boolean isInstSuccessfull = false;
		boolean isInstTableFailed = false;
		String orgQuery = input;
		orgQuery = orgQuery.trim();
		String query = input.toLowerCase().trim();

		tblName = orgQuery.substring(query.indexOf("into ")+5,query.indexOf(" values")).trim();

		String colNames = orgQuery.substring(query.indexOf("(")+1,query.indexOf(")"));
		String[] spacedColumnNames = colNames.split(",");
		String[] selectColumnNames = new String[spacedColumnNames.length];
		for (int m = 0; m < spacedColumnNames.length; m++)
			selectColumnNames[m] = spacedColumnNames[m].trim();

		SchemaHandler currentSchema =  SchemaHandler.getInstance();
		String currentSchemaName = currentSchema.getCurrentSchema();

		if(isTablePresent(currentSchemaName,tblName)){

			if(selectColumnNames.length == manager.getTblDeg(currentSchemaName, tblName)){
				InsertHelper insertHelper = InsertHelper.getInstance();
				isInstSuccessfull = insertHelper.insertValues(currentSchemaName, tblName,selectColumnNames,manager.getColumnSchema(currentSchemaName, tblName));
				if(isInstSuccessfull){
					updateInfochemaRow(currentSchemaName,InsertHelper.getRowCount());

					updateTableData(currentSchemaName,
							InsertHelper.getTableData(currentSchemaName,tblName),
							manager.getColumnSchema(currentSchemaName, tblName)
							);
					updateIndexTable(currentSchemaName,
							InsertHelper.getIndexData(currentSchemaName,tblName)
							);

				} else 
					isInstTableFailed = true;
			} else {
				System.out.println("Create schema, table and try again!...");
				isInstTableFailed = true;
			}
		}
		return isInstTableFailed;
	} 

	public void updateIndexTable(String currentSchema,
			TreeMap<String,TreeMap<String,List<String>>> indexMap
			){

		Set<Map.Entry<String,TreeMap<String,List<String>>>> indexMapSet = indexMap.entrySet();
		Iterator<Map.Entry<String,TreeMap<String,List<String>>>> indexIterator = indexMapSet.iterator();

		while(indexIterator.hasNext()){

			Map.Entry<String,TreeMap<String,List<String>>> columnEntry = indexIterator.next();
			String currentColumn = columnEntry.getKey();
			TreeMap<String,List<String>> currentColumnMap = columnEntry.getValue();

			Set<Map.Entry<String,List<String>>> currentColumnSet = currentColumnMap.entrySet();
			Iterator<Map.Entry<String,List<String>>> currentColumnIterator = currentColumnSet.iterator();

			while(currentColumnIterator.hasNext()){
				Map.Entry<String,List<String>> currentColumnMapList = currentColumnIterator.next();
				String columnStr = currentColumnMapList.getKey();
				List<String> columnStrList = currentColumnMapList.getValue();
				String openIndexFileName = currentSchema + "." + tblName + "." + currentColumn + ".tbl.ndx";
				try {
					RandomAccessFile openedIndexFile = new RandomAccessFile(openIndexFileName, "rw");
					openedIndexFile.seek(openedIndexFile.length());

					String type = columnStrList.get(0);
					String pointerCount = columnStrList.get(1);

					if(type.equalsIgnoreCase("CHAR") || type.equalsIgnoreCase("VARCHAR")){
						openedIndexFile.writeByte(columnStr.length());
						openedIndexFile.writeBytes(columnStr);
						openedIndexFile.writeInt(Integer.parseInt(pointerCount));
						for(int i = 0;i < Integer.parseInt(pointerCount); i++)
							openedIndexFile.writeInt(Integer.parseInt(columnStrList.get(i + 2)));		
					} else {
						switch(type){
						case "BYTE": 
							openedIndexFile.writeByte(Integer.parseInt(columnStr));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnStrList.get(i + 2)));
							break;
						case "SHORT":
							openedIndexFile.writeShort(Integer.parseInt(columnStr));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnStrList.get(i + 2)));
							break;
						case "INT": 
							openedIndexFile.writeInt(Integer.parseInt(columnStr));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnStrList.get(i + 2)));
							break;
						case "LONG": 
							openedIndexFile.writeLong(Integer.parseInt(columnStr));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnStrList.get(i + 2)));
							break;
						case "FLOAT": 
							openedIndexFile.writeFloat(Integer.parseInt(columnStr));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnStrList.get(i + 2)));
							break;
						case "DOUBLE": 
							openedIndexFile.writeDouble(Integer.parseInt(columnStr));
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnStrList.get(i + 2)));
							break;
						case "DATETIME": 	
							DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
							Date dateTime = dateTimeFormat.parse(columnStr);
							openedIndexFile.writeLong(dateTime.getTime());
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnStrList.get(i + 2)));
							break;
						case "DATE": 	
							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
							Date date = dateFormat.parse(columnStr);
							openedIndexFile.writeLong(date.getTime());							
							openedIndexFile.writeInt(Integer.parseInt(pointerCount));
							for(int i = 0;i < Integer.parseInt(pointerCount); i++)
								openedIndexFile.writeInt(Integer.parseInt(columnStrList.get(i + 2)));
							break;
						}
					}
					openedIndexFile.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	} 
	
	public void updateInfochemaRow(String currentSchemaName,int rowCount){
		try {
			RandomAccessFile tableFile = new RandomAccessFile("information_schema.table.tbl", "rw");
			tableFile.seek(0);

			while(tableFile.getFilePointer() < tableFile.length()){
				String readSchemaName = "";
				byte vcLength = tableFile.readByte();
				for(int j = 0; j < vcLength; j++)
					readSchemaName += (char)tableFile.readByte();

				if(readSchemaName.equals(currentSchemaName)){
					String readTableName = "";
					byte vcTableLength = tableFile.readByte();
					for(int j = 0; j < vcTableLength; j++)
						readTableName += (char)tableFile.readByte();

					if(readTableName.equals(tblName)){
						tableFile.writeLong(rowCount); 
						break;
					} else {
						tableFile.readLong();
					}
				} else {
					byte traverseLength = tableFile.readByte();
					for(int j = 0; j < traverseLength; j++)
						tableFile.readByte();
					tableFile.readLong();
				}	
			}
			tableFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	public void updateTableData(String currentSchema,
			TreeMap<Integer,List<String>> tableDataMapper,
			TreeMap<Integer,List<String>> tableSchemaMapper){
		try{
			InsertHelper iH = InsertHelper.getInstance();
			RandomAccessFile tableDataFile = new RandomAccessFile(currentSchema + "." + tblName + ".tbl", "rw");

			Set<Map.Entry<Integer,List<String>>> tableDataMapSet = tableDataMapper.entrySet();
			Iterator<Map.Entry<Integer,List<String>>> tableDataMapIterator = tableDataMapSet.iterator();

			Set<Map.Entry<Integer,List<String>>> columnSet = tableSchemaMapper.entrySet();
			Iterator<Map.Entry<Integer,List<String>>> columnIterator = columnSet.iterator();
			List<String> columnSchema = new ArrayList<String>();

			while(columnIterator.hasNext()){
				Map.Entry<Integer,List<String>> columnME = columnIterator.next();
				List<String> currentColumn = columnME.getValue();
				columnSchema.add(currentColumn.get(0));
				columnSchema.add(currentColumn.get(1));
			}

			while(tableDataMapIterator.hasNext()){
				Map.Entry<Integer,List<String>> columnME = tableDataMapIterator.next();

				List<String> currentColumn = columnME.getValue();
				int columnDataCount = currentColumn.size();
				int columnSchemaCount = 0;

				for(int i = 0;i < columnDataCount; i++){
					long tableIndexPointer = tableDataFile.getFilePointer();
					if(columnSchema.get(columnSchemaCount + 1).contains("VARCHAR")){
						tableDataFile.writeByte(currentColumn.get(i).length());
						tableDataFile.writeBytes(currentColumn.get(i));
						iH.updateIndex(currentSchema, tblName, columnSchema.get(columnSchemaCount), tableIndexPointer,columnSchema.get(columnSchemaCount + 1), currentColumn.get(i));
						columnSchemaCount = columnSchemaCount + 2;
					} else {
						switch(columnSchema.get(columnSchemaCount + 1)){
						case "CHAR":
							tableDataFile.writeByte(currentColumn.get(i).length());
							tableDataFile.writeBytes(currentColumn.get(i));
							iH.updateIndex(currentSchema, tblName, columnSchema.get(columnSchemaCount), tableIndexPointer,columnSchema.get(columnSchemaCount + 1), currentColumn.get(i));
							columnSchemaCount = columnSchemaCount + 2;
							break;
						case "BYTE": 
							tableDataFile.writeBytes(currentColumn.get(i));
							iH.updateIndex(currentSchema, tblName, columnSchema.get(columnSchemaCount), tableIndexPointer,columnSchema.get(columnSchemaCount + 1), currentColumn.get(i));
							columnSchemaCount = columnSchemaCount + 2;
							break;
						case "SHORT":
							tableDataFile.writeShort(Integer.parseInt(currentColumn.get(i)));
							iH.updateIndex(currentSchema, tblName, columnSchema.get(columnSchemaCount), tableIndexPointer,columnSchema.get(columnSchemaCount + 1), currentColumn.get(i));
							columnSchemaCount = columnSchemaCount + 2;
							break;
						case "INT":
							tableDataFile.writeInt(Integer.parseInt(currentColumn.get(i)));
							iH.updateIndex(currentSchema, tblName, columnSchema.get(columnSchemaCount), tableIndexPointer,columnSchema.get(columnSchemaCount + 1), currentColumn.get(i));
							columnSchemaCount = columnSchemaCount + 2;
							break;
						case "LONG": 
							tableDataFile.writeLong(Integer.parseInt(currentColumn.get(i)));
							iH.updateIndex(currentSchema, tblName, columnSchema.get(columnSchemaCount), tableIndexPointer,columnSchema.get(columnSchemaCount + 1), currentColumn.get(i));
							columnSchemaCount = columnSchemaCount + 2;
							break;
						case "FLOAT": 
							tableDataFile.writeFloat(Integer.parseInt(currentColumn.get(i)));
							iH.updateIndex(currentSchema, tblName, columnSchema.get(columnSchemaCount), tableIndexPointer,columnSchema.get(columnSchemaCount + 1), currentColumn.get(i));
							columnSchemaCount = columnSchemaCount + 2;
							break;
						case "DOUBLE": 
							tableDataFile.writeDouble(Integer.parseInt(currentColumn.get(i)));
							iH.updateIndex(currentSchema, tblName, columnSchema.get(columnSchemaCount), tableIndexPointer,columnSchema.get(columnSchemaCount + 1), currentColumn.get(i));
							columnSchemaCount = columnSchemaCount + 2;
							break;
						case "DATETIME": 
							DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
							Date dateTime = dateTimeFormat.parse(currentColumn.get(i));
							tableDataFile.writeLong(dateTime.getTime());
							iH.updateIndex(currentSchema, tblName, columnSchema.get(columnSchemaCount), tableIndexPointer,columnSchema.get(columnSchemaCount + 1), currentColumn.get(i));
							columnSchemaCount = columnSchemaCount + 2;
							break;
						case "DATE": 
							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
							Date date = dateFormat.parse(currentColumn.get(i));
							tableDataFile.writeLong(date.getTime());
							iH.updateIndex(currentSchema, tblName, columnSchema.get(columnSchemaCount), tableIndexPointer,columnSchema.get(columnSchemaCount + 1), currentColumn.get(i));
							columnSchemaCount = columnSchemaCount + 2;
							break;
						}
					}
				}
			}			
			tableDataFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	} 

	public void selectTable(int selectType, String input){
		String orgQuery = input;
		orgQuery = orgQuery.trim();
		String query = input.toLowerCase().trim();
		SchemaHandler currSchema =  SchemaHandler.getInstance();
		String currSchemaName = currSchema.getCurrentSchema();
		String whereCondition = "";
		String Op[] = {"<=",">=","<",">","="};
		String[] whereStr = new String[2];
		String workStr = "";
		String columnType = "";
		boolean isColumnFound = false;
		int columnIndex = 0;

		switch(selectType){
		case 1:
			tblName = orgQuery.substring(query.indexOf("from ")+5).trim();

			if(currSchemaName.equals("information_schema")){
				if(tblName.equals("SCHEMATA"))
					currSchema.showSchema();
				else if(tblName.equals("TABLES"))
					currSchema.selectInfoSchemTable();
				else if(tblName.equals("COLUMNS"))
					currSchema.selectInfoSchemaColumn();
			}else if(isTablePresent(currSchemaName,tblName)){

				TreeMap<Integer,List<String>> tableDataMap = InsertHelper.getTableData(currSchemaName,tblName);
				Set<Map.Entry<Integer,List<String>>> tableDataMapSet = tableDataMap.entrySet();
				Iterator<Map.Entry<Integer,List<String>>> tableDataMapIterator = tableDataMapSet.iterator();

				while(tableDataMapIterator.hasNext()){
					Map.Entry<Integer,List<String>> columnMapEntry = tableDataMapIterator.next();
					List<String> curRow = columnMapEntry.getValue();
					showSelectValues(curRow);
				}
			} 
			break;
		case 2:
			tblName = orgQuery.substring(query.indexOf("from ")+5,query.indexOf(" where")).trim();
			if(currSchemaName.equals("information_schema")){
				if(tblName.equals("SCHEMATA"))
					currSchema.showSchema();
				else if(tblName.equals("TABLES"))
					currSchema.selectInfoSchemTable();
				else if(tblName.equals("COLUMNS"))
					currSchema.selectInfoSchemaColumn();
			} else {
				whereCondition = orgQuery.substring(query.indexOf("where ") + 6);
				String operator = "";
				int i = 0;	
				

				for(i = 0; i < Op.length; i++){
					if(whereCondition.contains(Op[i])){
						workStr = (whereCondition.trim());
						break;
					}
				}
				operator = Op[i];
				whereStr = workStr.split(operator);

				for(int j = 0; j < whereStr.length ;j++){
					whereStr[j] = whereStr[j].trim();
				}

				if(isTablePresent(currSchemaName,tblName)){
					
					TableSchemaManager tsm = TableSchemaManager.getInstance();
					TreeMap<Integer,List<String>> tableSchemaMap =	tsm.getColumnSchema(currSchemaName, tblName);
					Set<Map.Entry<Integer,List<String>>> tableSchemaSet = tableSchemaMap.entrySet();
					Iterator<Map.Entry<Integer,List<String>>> tableSchemaIterator = tableSchemaSet.iterator();

					while(tableSchemaIterator.hasNext()){
						Map.Entry<Integer,List<String>> columnMapEntry = tableSchemaIterator.next();
						List<String> columnV = columnMapEntry.getValue();
						if(columnV.contains(whereStr[0])){
							columnIndex = columnMapEntry.getKey();
							columnType = columnV.get(1);
							isColumnFound = true;
							break;	
						} 
					}

					if(isColumnFound){
						TreeMap<Integer,List<String>> tableDataMap = InsertHelper.getTableData(currSchemaName,tblName);
						Set<Map.Entry<Integer,List<String>>> tableDataMapSet = tableDataMap.entrySet();
						Iterator<Map.Entry<Integer,List<String>>> tableDataMapIterator = tableDataMapSet.iterator();

						while(tableDataMapIterator.hasNext()){
							Map.Entry<Integer,List<String>> columnMapEntry = tableDataMapIterator.next();
							List<String> currRow = columnMapEntry.getValue();
							switch(operator){
							case "<=":
								if(lessthanEqualCheck(currRow, columnIndex - 1, columnType, whereStr[1]))
									showSelectValues(currRow);
								break;
							case ">=":
								if(greaterThanEqualCheck(currRow, columnIndex - 1, columnType, whereStr[1]))
									showSelectValues(currRow);
								break;
							case "<":
								if(lessThanCheck(currRow, columnIndex - 1, columnType, whereStr[1]))
									showSelectValues(currRow);
								break;
							case ">":
								if(greaterThanCheck(currRow, columnIndex - 1, columnType, whereStr[1]))
									showSelectValues(currRow);
								break;
							case "=":
								if(equalCheck(currRow, columnIndex - 1, columnType, whereStr[1]))
									showSelectValues(currRow);
								break;
							}
						} 
					}else {
						System.out.println("Unknown column '" + whereStr[0] + "' in 'where clause'");
					}
				}
			}
			break;
		case 3:
			tblName = orgQuery.substring(query.indexOf("from ")+5,query.indexOf(" where")).trim();
			if(currSchemaName.equals("information_schema")){
				if(tblName.equals("SCHEMATA"))
					currSchema.showSchema();
				else if(tblName.equals("TABLES"))
					currSchema.selectInfoSchemTable();
				else if(tblName.equals("COLUMNS"))
					currSchema.selectInfoSchemaColumn();
			} else {
				whereCondition = orgQuery.substring(query.indexOf("where ") + 6, query.indexOf(" is")).trim();

				if(isTablePresent(currSchemaName,tblName)){
					TableSchemaManager tsm = TableSchemaManager.getInstance();
					TreeMap<Integer,List<String>> tableSchemaMap =	tsm.getColumnSchema(currSchemaName, tblName);
					Set<Map.Entry<Integer,List<String>>> tableSchemaSet = tableSchemaMap.entrySet();
					Iterator<Map.Entry<Integer,List<String>>> tableSchemaIterator = tableSchemaSet.iterator();

					while(tableSchemaIterator.hasNext()){
						Map.Entry<Integer,List<String>> columnMapEntry = tableSchemaIterator.next();
						List<String> columnListStr = columnMapEntry.getValue();
						if(columnListStr.contains(whereCondition)){
							columnIndex = columnMapEntry.getKey();
							columnType = columnListStr.get(1);
							isColumnFound = true;
							break;	
						} 
					}

					if(isColumnFound){
						TreeMap<Integer,List<String>> tableDataMap = InsertHelper.getTableData(currSchemaName,tblName);
						Set<Map.Entry<Integer,List<String>>> tableDataMapSet = tableDataMap.entrySet();
						Iterator<Map.Entry<Integer,List<String>>> tableDataMapIterator = tableDataMapSet.iterator();

						while(tableDataMapIterator.hasNext()){
							Map.Entry<Integer,List<String>> columnMapEntry = tableDataMapIterator.next();
							List<String> currRow = columnMapEntry.getValue();
							if(currRow.get(columnIndex - 1).equalsIgnoreCase("NULL") || (currRow.get(columnIndex - 1) == ""))
								showSelectValues(currRow);
						} 
					}else {
						System.out.println("Unknown column '" + whereCondition + "' in 'where clause'");
					}
				}
			}
			break;
		case 4:
			tblName = orgQuery.substring(query.indexOf("from ")+5,query.indexOf(" where")).trim();
			if(currSchemaName.equals("information_schema")){
				if(tblName.equals("SCHEMATA"))
					currSchema.showSchema();
				else if(tblName.equals("TABLES"))
					currSchema.selectInfoSchemTable();
				else if(tblName.equals("COLUMNS"))
					currSchema.selectInfoSchemaColumn();
			} else {
				whereCondition = orgQuery.substring(query.indexOf("where ") + 6, query.indexOf(" is")).trim();

				if(isTablePresent(currSchemaName,tblName)){
					TableSchemaManager tsm = TableSchemaManager.getInstance();
					TreeMap<Integer,List<String>> tableSchemaMap =	tsm.getColumnSchema(currSchemaName, tblName);
					Set<Map.Entry<Integer,List<String>>> tableSchemaSet = tableSchemaMap.entrySet();
					Iterator<Map.Entry<Integer,List<String>>> tableSchemaIterator = tableSchemaSet.iterator();

					while(tableSchemaIterator.hasNext()){
						Map.Entry<Integer,List<String>> columnMapEntry = tableSchemaIterator.next();
						List<String> columnListStr = columnMapEntry.getValue();
						if(columnListStr.contains(whereCondition)){
							columnIndex = columnMapEntry.getKey();
							columnType = columnListStr.get(1);
							isColumnFound = true;
							break;	
						} 
					}	
					if(isColumnFound){
						TreeMap<Integer,List<String>> tableDataMap = InsertHelper.getTableData(currSchemaName,tblName);
						Set<Map.Entry<Integer,List<String>>> tableDataMapSet = tableDataMap.entrySet();
						Iterator<Map.Entry<Integer,List<String>>> tableDataMapIterator = tableDataMapSet.iterator();

						while(tableDataMapIterator.hasNext()){
							Map.Entry<Integer,List<String>> columnMapEntry = tableDataMapIterator.next();
							List<String> currRow = columnMapEntry.getValue();
							if(!(currRow.get(columnIndex - 1).equalsIgnoreCase("NULL") || currRow.get(columnIndex - 1) == ""))
								showSelectValues(currRow);
						} 
					}else {
						System.out.println("Unknown column '" + whereCondition + "' in 'where clause'");
					}
				}
			}
			break;	
		}
	} 

	public void showSelectValues(List<String> rowValues){
		for(int i = 0; i < rowValues.size() ; i++){
			if(i == rowValues.size() - 1)
				System.out.print(rowValues.get(i));
			else
				System.out.print(rowValues.get(i) + ",");
		}
		System.out.println();
	} 
	public boolean lessthanEqualCheck(List<String> row, int idx, String type,String operand){
		boolean result = false;
		try{
			switch(type){
			case "VARCHAR":
			case "CHAR":
				if(row.get(idx).compareTo(operand) <= 0)
					result = true;
				break;
			case "BYTE": 
				if(Byte.parseByte(row.get(idx)) <= Byte.parseByte(operand))
					result = true;
				break;
			case "SHORT":
				if(Short.parseShort(row.get(idx)) <= Short.parseShort(operand))
					result = true;
				break;
			case "INT":
				if(Integer.parseInt(row.get(idx)) <= Integer.parseInt(operand))
					result = true;
				break;
			case "LONG": 
				if(Long.parseLong(row.get(idx)) <= Long.parseLong(operand))
					result = true;	
				break;
			case "FLOAT": 
				if(Float.parseFloat(row.get(idx)) <= Float.parseFloat(operand))
					result = true;
				break;
			case "DOUBLE": 
				if(Double.parseDouble(row.get(idx)) <= Double.parseDouble(operand))
					result = true;
				break;
			case "DATETIME": 
				DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date src = dateTimeFormat.parse(row.get(idx));
				Date dst = dateTimeFormat.parse(operand);
				if(src.before(dst) || src.equals(dst))
					result = true;
				break;
			case "DATE": 
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date src2 = dateFormat.parse(row.get(idx));
				Date dst2 = dateFormat.parse(operand);
				if(src2.before(dst2) || src2.equals(dst2))
					result = true;
				break;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}

	
	public boolean greaterThanEqualCheck(List<String> row, int idx, String type,String operand){
		boolean result = false;
		try{
			switch(type){
			case "VARCHAR":
			case "CHAR":
				if(row.get(idx).compareTo(operand) >= 0)
					result = true;
				break;
			case "BYTE": 
				if(Byte.parseByte(row.get(idx)) >= Byte.parseByte(operand))
					result = true;
				break;
			case "SHORT":
				if(Short.parseShort(row.get(idx)) >= Short.parseShort(operand))
					result = true;
				break;
			case "INT":
				if(Integer.parseInt(row.get(idx)) >= Integer.parseInt(operand))
					result = true;
				break;
			case "LONG": 
				if(Long.parseLong(row.get(idx)) >= Long.parseLong(operand))
					result = true;	
				break;
			case "FLOAT": 
				if(Float.parseFloat(row.get(idx)) >= Float.parseFloat(operand))
					result = true;
				break;
			case "DOUBLE": 
				if(Double.parseDouble(row.get(idx)) >= Double.parseDouble(operand))
					result = true;
				break;
			case "DATETIME": 
				DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date src = dateTimeFormat.parse(row.get(idx));
				Date dst = dateTimeFormat.parse(operand);
				if(src.after(dst) || src.equals(dst))
					result = true;
				break;
			case "DATE": 
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date src2 = dateFormat.parse(row.get(idx));
				Date dst2 = dateFormat.parse(operand);
				if(src2.after(dst2) || src2.equals(dst2))
					result = true;
				break;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}

	
	public boolean lessThanCheck(List<String> row, int idx, String type,String operand){
		boolean result = false;
		try{
			switch(type){
			case "VARCHAR":
			case "CHAR":
				if(row.get(idx).compareTo(operand) < 0)
					result = true;
				break;
			case "BYTE": 
				if(Byte.parseByte(row.get(idx)) < Byte.parseByte(operand))
					result = true;
				break;
			case "SHORT":
				if(Short.parseShort(row.get(idx)) < Short.parseShort(operand))
					result = true;
				break;
			case "INT":
				if(Integer.parseInt(row.get(idx)) < Integer.parseInt(operand))
					result = true;
				break;
			case "LONG": 
				if(Long.parseLong(row.get(idx)) < Long.parseLong(operand))
					result = true;	
				break;
			case "FLOAT": 
				if(Float.parseFloat(row.get(idx)) < Float.parseFloat(operand))
					result = true;
				break;
			case "DOUBLE": 
				if(Double.parseDouble(row.get(idx)) < Double.parseDouble(operand))
					result = true;
				break;
			case "DATETIME": 
				DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date src = dateTimeFormat.parse(row.get(idx));
				Date dst = dateTimeFormat.parse(operand);
				if(src.before(dst))
					result = true;
				break;
			case "DATE": 
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date src2 = dateFormat.parse(row.get(idx));
				Date dst2 = dateFormat.parse(operand);
				if(src2.before(dst2))
					result = true;
				break;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}

	
	public boolean greaterThanCheck(List<String> row, int idx, String type,String operand){
		boolean result = false;
		try{
			switch(type){
			case "VARCHAR":
			case "CHAR":
				if(row.get(idx).compareTo(operand) > 0)
					result = true;
				break;
			case "BYTE": 
				if(Byte.parseByte(row.get(idx)) > Byte.parseByte(operand))
					result = true;
				break;
			case "SHORT":
				if(Short.parseShort(row.get(idx)) > Short.parseShort(operand))
					result = true;
				break;
			case "INT":
				if(Integer.parseInt(row.get(idx)) > Integer.parseInt(operand))
					result = true;
				break;
			case "LONG": 
				if(Long.parseLong(row.get(idx)) > Long.parseLong(operand))
					result = true;	
				break;
			case "FLOAT": 
				if(Float.parseFloat(row.get(idx)) > Float.parseFloat(operand))
					result = true;
				break;
			case "DOUBLE": 
				if(Double.parseDouble(row.get(idx)) > Double.parseDouble(operand))
					result = true;
				break;
			case "DATETIME": 
				DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date src = dateTimeFormat.parse(row.get(idx));
				Date dst = dateTimeFormat.parse(operand);
				if(src.after(dst))
					result = true;
				break;
			case "DATE": 
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date src2 = dateFormat.parse(row.get(idx));
				Date dst2 = dateFormat.parse(operand);
				if(src2.after(dst2))
					result = true;
				break;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return result;
	}


	public boolean equalCheck(List<String> row, int idx, String type,String operand){
		boolean result = false;
		try{
			switch(type){
			case "VARCHAR":
			case "CHAR":
				if(row.get(idx).compareTo(operand) == 0)
					result = true;
				break;
			case "BYTE": 
				if(Byte.parseByte(row.get(idx)) == Byte.parseByte(operand))
					result = true;
				break;
			case "SHORT":
				if(Short.parseShort(row.get(idx)) == Short.parseShort(operand))
					result = true;
				break;
			case "INT":
				if(Integer.parseInt(row.get(idx)) == Integer.parseInt(operand))
					result = true;
				break;
			case "LONG": 
				if(Long.parseLong(row.get(idx)) == Long.parseLong(operand))
					result = true;	
				break;
			case "FLOAT": 
				if(Float.parseFloat(row.get(idx)) == Float.parseFloat(operand))
					result = true;
				break;
			case "DOUBLE": 
				if(Double.parseDouble(row.get(idx)) == Double.parseDouble(operand))
					result = true;
				break;
			case "DATETIME": 
				DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss", Locale.ENGLISH);
				Date src = dateTimeFormat.parse(row.get(idx));
				Date dst = dateTimeFormat.parse(operand);
				if(src.equals(dst))
					result = true;
				break;
			case "DATE": 
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
				Date src2 = dateFormat.parse(row.get(idx));
				Date dst2 = dateFormat.parse(operand);
				if(src2.equals(dst2))
					result = true;
				break;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}

	public boolean updateTable(String input) {
	
		TableSchemaManager tsm = TableSchemaManager.getInstance();
		boolean isUpdateSuccessfull = false;
		boolean isUpdateTableFailed = false;
		String orgQuery = input;
		orgQuery = orgQuery.trim();
		String query = input.toLowerCase().trim();
		
		

		tblName = orgQuery.substring(query.indexOf("into ")+5,query.indexOf(" values")).trim();

		String colNames = orgQuery.substring(query.indexOf("(")+1,query.indexOf(")"));
		String[] spacedColNames = colNames.split(",");
		String[] selectColNames = new String[spacedColNames.length];
		
		for (int m = 0; m < spacedColNames.length; m++)
			selectColNames[m] = spacedColNames[m].trim();

		SchemaHandler currentSchema =  SchemaHandler.getInstance();
		String currentSchemaName = currentSchema.getCurrentSchema();

		if(isTablePresent(currentSchemaName,tblName)){

			if(selectColNames.length == tsm.getTblDeg(currentSchemaName, tblName)){
				InsertHelper insertHelper = InsertHelper.getInstance();
				isUpdateSuccessfull = insertHelper.updateValues(currentSchemaName, tblName,selectColNames,tsm.getColumnSchema(currentSchemaName, tblName));
				if(isUpdateSuccessfull){
					updateInfochemaRow(currentSchemaName,InsertHelper.getRowCount());

					updateTableData(currentSchemaName,
							InsertHelper.getTableData(currentSchemaName,tblName),
							tsm.getColumnSchema(currentSchemaName, tblName)
							);

					updateIndexTable(currentSchemaName,
							InsertHelper.getIndexData(currentSchemaName,tblName)
							);

				} else 
					isUpdateTableFailed = true;
			} else {
				System.out.println("Create schema, table and try again!");
				isUpdateTableFailed = true;
			}
		}
		return isUpdateTableFailed;
	
	}
}
