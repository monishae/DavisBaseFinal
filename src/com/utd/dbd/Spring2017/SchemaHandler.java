package com.utd.dbd.Spring2017;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;


public class SchemaHandler{

	
	private SchemaHandler(){}

	private static SchemaHandler instance;

	public static SchemaHandler getInstance(){
		if(instance == null)
			return new SchemaHandler();

		return instance;
	}

	public static String currentSchema = "information_schema";
	
	public void setCurrentSchema(String schema){
		SchemaHandler.currentSchema = schema;
	}

	
	public String getCurrentSchema(){
		return SchemaHandler.currentSchema;
	}

	
	public void showSchema(){
		int schCount = 1;
		try {
			RandomAccessFile schFile = new RandomAccessFile("information_schema.schemata.tbl", "rw");

			System.out.println(SystemDescription.line("-",50));	
			System.out.println("No.\tSchema Name");
			System.out.println(SystemDescription.line("-",50));


			while(schFile.getFilePointer() < schFile.length()){
				System.out.print(schCount++ + "\t");
				byte vcLength = schFile.readByte();
				for(int j = 0; j < vcLength; j++)
					System.out.print((char)schFile.readByte());
				System.out.print("\n");
			}

			System.out.println(SystemDescription.line("-",50));
			 
			schFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (EOFException e){
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}  catch (Exception e){
			e.printStackTrace();
		}	
	}

	

	
	public void useSchema(String input){
		String query = input;
		query = query.trim();
		input = input.toLowerCase().trim();
		String schName = query.substring(input.indexOf("use ")+4,input.length()).trim();
		boolean isPresent = true;
		try {

			RandomAccessFile schFile = new RandomAccessFile("information_schema.schemata.tbl","r");

			while(schFile.getFilePointer() < schFile.length()){
				String getSchemaName = "";
				byte vcLength = schFile.readByte();
				for(int j = 0; j < vcLength; j++)
					getSchemaName += (char)schFile.readByte();
				if(getSchemaName.equals(schName)){
					this.setCurrentSchema(schName);
					System.out.println("Current Schema: " + this.getCurrentSchema() + "\nDatabase Changed");
					schFile.close();
					isPresent = false;
					break;
				} 
				
			}

			if(isPresent){
				System.out.println("Schema File Not found!");
				schFile.close();
				return;
			} 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	public void createSchema(String input){
		String query = input;
		query = query.trim();
		input = input.toLowerCase().trim();
		String schName = query.substring(input.indexOf("schema ")+7,input.length()).trim();
		boolean isPresent = false;
		try {

			RandomAccessFile schFile = new RandomAccessFile("information_schema.schemata.tbl","rw");
			while(schFile.getFilePointer() < schFile.length()){
				String readSchemaName = "";
				byte vcLength = schFile.readByte();
				for(int j = 0; j < vcLength; j++)
					readSchemaName += (char)schFile.readByte();

				if(readSchemaName.equals(schName)){
					System.out.println("Can't create database '" + schName + "'\nDatabase exists!");
					isPresent = true;
					break;
				} 	
			}

			schFile.seek(schFile.length());
			if(!isPresent){
				schFile.writeByte(schName.length());
				schFile.writeBytes(schName);
				System.out.println("Schema '" + schName + "'created successfully!");
			}

		
			schFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

	
	public void selectInfoSchemTable(){
		try {
			RandomAccessFile tblFile = new RandomAccessFile("information_schema.table.tbl", "rw");

			while ( tblFile.getFilePointer() < tblFile.length() ) {
				String getSchName = "";
				String tblName = "";
				int rowCount = 0;
				byte vcLength = tblFile.readByte();
				for(int j = 0; j < vcLength; j++)
					getSchName += (char)tblFile.readByte();

				byte vtLength = tblFile.readByte();
				for(int k = 0; k < vtLength; k++)
					tblName += (char)tblFile.readByte();

				rowCount = (int)tblFile.readLong();

				System.out.println(getSchName + "," + tblName + "," + rowCount);
			}
			tblFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void selectInfoSchemaColumn(){
		try {
			RandomAccessFile columnsFile = new RandomAccessFile("information_schema.columns.tbl", "rw");
			while ( columnsFile.getFilePointer() < columnsFile.length() ) {
				String getSchemaName = "";
				String getTableName = "";
				String getColumnName = "";
				int ordPos = 0;
				String colType = "";
				String isNull = "";
				String isPK = "";
				
				
				byte vcLength = columnsFile.readByte();
				for(int j = 0; j < vcLength; j++)
					getSchemaName += (char)columnsFile.readByte();
				
				
				byte vtLength = columnsFile.readByte();
				for(int k = 0; k < vtLength; k++)
					getTableName += (char)columnsFile.readByte();

				
				byte varColLength = columnsFile.readByte();
				for(int k = 0; k < varColLength; k++)
					getColumnName += (char)columnsFile.readByte();
				
				
				ordPos = columnsFile.readInt();

				
				byte vTypeLength = columnsFile.readByte();
				for(int k = 0; k < vTypeLength; k++)
					colType += (char)columnsFile.readByte();
				
				
				byte vNLength = columnsFile.readByte();
				for(int k = 0; k < vNLength; k++)
					isNull += (char)columnsFile.readByte();
				
				
				byte vPKLength = columnsFile.readByte();
				for(int k = 0; k < vPKLength; k++)
					isPK += (char)columnsFile.readByte();	
				if(isPK.equals("")) isPK = "NO";
				
				System.out.println(getSchemaName + "," + getTableName + "," +getColumnName + "," +ordPos + "," +colType + "," +isNull + "," +isPK);
			}
			columnsFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public boolean initInformationSchema(){
		boolean isInitialized = false;
		try {
			RandomAccessFile schemaTblFile = new RandomAccessFile("information_schema.schemata.tbl", "rw");
			RandomAccessFile tablesTblFile = new RandomAccessFile("information_schema.table.tbl", "rw");
			RandomAccessFile colTblFile = new RandomAccessFile("information_schema.columns.tbl", "rw");

			schemaTblFile.writeByte("information_schema".length());
			schemaTblFile.writeBytes("information_schema");

			tablesTblFile.writeByte("information_schema".length()); 
			tablesTblFile.writeBytes("information_schema");
			tablesTblFile.writeByte("SCHEMATA".length()); 
			tablesTblFile.writeBytes("SCHEMATA");
			tablesTblFile.writeLong(1);

			tablesTblFile.writeByte("information_schema".length());
			tablesTblFile.writeBytes("information_schema");
			tablesTblFile.writeByte("TABLES".length()); 
			tablesTblFile.writeBytes("TABLES");
			tablesTblFile.writeLong(3);

			tablesTblFile.writeByte("information_schema".length()); 
			tablesTblFile.writeBytes("information_schema");
			tablesTblFile.writeByte("COLUMNS".length());
			tablesTblFile.writeBytes("COLUMNS");
			tablesTblFile.writeLong(7);

			
			colTblFile.writeByte("information_schema".length()); 
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("SCHEMATA".length()); 
			colTblFile.writeBytes("SCHEMATA");
			colTblFile.writeByte("SCHEMA_NAME".length());
			colTblFile.writeBytes("SCHEMA_NAME");
			colTblFile.writeInt(1); 
			colTblFile.writeByte("varchar(64)".length()); 
			colTblFile.writeBytes("varchar(64)");
			colTblFile.writeByte("NO".length()); 
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length()); 
			colTblFile.writeBytes("");

			
			colTblFile.writeByte("information_schema".length());
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("TABLES".length()); 
			colTblFile.writeBytes("TABLES");
			colTblFile.writeByte("TABLE_SCHEMA".length()); 
			colTblFile.writeBytes("TABLE_SCHEMA");
			colTblFile.writeInt(1);
			colTblFile.writeByte("varchar(64)".length()); 
			colTblFile.writeBytes("varchar(64)");
			colTblFile.writeByte("NO".length());
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length());
			colTblFile.writeBytes("");

			colTblFile.writeByte("information_schema".length()); 
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("TABLES".length()); 
			colTblFile.writeBytes("TABLES");
			colTblFile.writeByte("TABLE_NAME".length());
			colTblFile.writeBytes("TABLE_NAME");
			colTblFile.writeInt(2); 
			colTblFile.writeByte("varchar(64)".length());
			colTblFile.writeBytes("varchar(64)");
			colTblFile.writeByte("NO".length()); 
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length()); 
			colTblFile.writeBytes("");

			colTblFile.writeByte("information_schema".length());
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("TABLES".length()); 
			colTblFile.writeBytes("TABLES");
			colTblFile.writeByte("TABLE_ROWS".length());
			colTblFile.writeBytes("TABLE_ROWS");
			colTblFile.writeInt(3);
			colTblFile.writeByte("long int".length());
			colTblFile.writeBytes("long int");
			colTblFile.writeByte("NO".length()); 
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length()); 
			colTblFile.writeBytes("");

			colTblFile.writeByte("information_schema".length()); 
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("COLUMNS".length());
			colTblFile.writeBytes("COLUMNS");
			colTblFile.writeByte("TABLE_SCHEMA".length()); 
			colTblFile.writeBytes("TABLE_SCHEMA");
			colTblFile.writeInt(1); 
			colTblFile.writeByte("varchar(64)".length()); 
			colTblFile.writeBytes("varchar(64)");
			colTblFile.writeByte("NO".length()); 
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length()); 
			colTblFile.writeBytes("");

			colTblFile.writeByte("information_schema".length()); 
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("COLUMNS".length()); 
			colTblFile.writeBytes("COLUMNS");
			colTblFile.writeByte("TABLE_NAME".length()); 
			colTblFile.writeBytes("TABLE_NAME");
			colTblFile.writeInt(2); 
			colTblFile.writeByte("varchar(64)".length()); 
			colTblFile.writeBytes("varchar(64)");
			colTblFile.writeByte("NO".length()); 
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length()); 
			colTblFile.writeBytes("");

			
			colTblFile.writeByte("information_schema".length()); 
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("COLUMNS".length()); 
			colTblFile.writeBytes("COLUMNS");
			colTblFile.writeByte("COLUMN_NAME".length()); 
			colTblFile.writeBytes("COLUMN_NAME");
			colTblFile.writeInt(3); 
			colTblFile.writeByte("varchar(64)".length());
			colTblFile.writeBytes("varchar(64)");
			colTblFile.writeByte("NO".length()); 
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length()); 
			colTblFile.writeBytes("");

			colTblFile.writeByte("information_schema".length());
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("COLUMNS".length()); 
			colTblFile.writeBytes("COLUMNS");
			colTblFile.writeByte("ORDINAL_POSITION".length()); 
			colTblFile.writeBytes("ORDINAL_POSITION");
			colTblFile.writeInt(4); 
			colTblFile.writeByte("int".length()); 
			colTblFile.writeBytes("int");
			colTblFile.writeByte("NO".length()); 
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length()); 
			colTblFile.writeBytes("");

			colTblFile.writeByte("information_schema".length()); 
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("COLUMNS".length()); 
			colTblFile.writeBytes("COLUMNS");
			colTblFile.writeByte("COLUMN_TYPE".length());
			colTblFile.writeBytes("COLUMN_TYPE");
			colTblFile.writeInt(5); 
			colTblFile.writeByte("varchar(64)".length()); 
			colTblFile.writeBytes("varchar(64)");
			colTblFile.writeByte("NO".length()); 
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length()); 
			colTblFile.writeBytes("");

			colTblFile.writeByte("information_schema".length());
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("COLUMNS".length()); 
			colTblFile.writeBytes("COLUMNS");
			colTblFile.writeByte("IS_NULLABLE".length()); 
			colTblFile.writeBytes("IS_NULLABLE");
			colTblFile.writeInt(6); 
			colTblFile.writeByte("varchar(3)".length()); 
			colTblFile.writeBytes("varchar(3)");
			colTblFile.writeByte("NO".length()); 
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length()); 
			colTblFile.writeBytes("");

			colTblFile.writeByte("information_schema".length()); 
			colTblFile.writeBytes("information_schema");
			colTblFile.writeByte("COLUMNS".length()); 
			colTblFile.writeBytes("COLUMNS");
			colTblFile.writeByte("COLUMN_KEY".length());
			colTblFile.writeBytes("COLUMN_KEY");
			colTblFile.writeInt(7); 
			colTblFile.writeByte("varchar(3)".length()); 
			colTblFile.writeBytes("varchar(3)");
			colTblFile.writeByte("NO".length()); 
			colTblFile.writeBytes("NO");
			colTblFile.writeByte("".length()); 
			colTblFile.writeBytes("");

			isInitialized = true;
			schemaTblFile.close();
			tablesTblFile.close();
			colTblFile.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}  

		return isInitialized;
	}

}
