package com.utd.dbd.Spring2017;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.Scanner;


public class DavisBaseApp {
	static BufferedReader queryReader = new BufferedReader(new InputStreamReader(System.in));
	static final String IS_SCHEMATA_FILE = "information_schema.schemata.tbl";
	static final String IS_TABLE_FILE = "information_schema.table.tbl";
	static final String IS_COLUMN_FILE = "information_schema.columns.tbl";

	public static void main(String[] args) {

		String currentDirectory = System.getProperty("user.dir");
		File activeDirectory = new File(currentDirectory);
		boolean startDavisBase = false;
		boolean createTblFailed = false;
		boolean insertTblFailed = false;
		boolean updateTblFailed = false;
		SchemaHandler schemaHandler = SchemaHandler.getInstance();
		TableHandler tableHandler = new TableHandler();
		try {
			//Checking whether schemata file is present or not 
			FilenameFilter schemataFilter = new FilenameFilter(){
				public boolean accept(File workingDirectory, String fileName){
					return fileName.equals(IS_SCHEMATA_FILE);
				}
			};
			String[] informationSchemaSchemata = activeDirectory.list(schemataFilter);

			//Checking whether information table file is present or not 
			FilenameFilter informationSchemaTableFilter = new FilenameFilter(){
				public boolean accept(File workingDirectory, String fileName){
					return fileName.equals(IS_TABLE_FILE);
				}
			};
			String[] informationSchemaTable = activeDirectory.list(informationSchemaTableFilter);

			//Checking whether information column file is present or not 
			FilenameFilter informationSchemaColumnFilter = new FilenameFilter(){
				public boolean accept(File workingDirectory, String fileName){
					return fileName.equals(IS_COLUMN_FILE);
				}
			};
			String[] informationSchemaColumn = activeDirectory.list(informationSchemaColumnFilter);

			if((informationSchemaSchemata.length == 1) && (informationSchemaTable.length == 1) && (informationSchemaColumn.length == 1)){
				System.out.println("Information Schema Found");
				startDavisBase = true;
			} else if(schemaHandler.initInformationSchema()){
				System.out.println("Initialized...");
				startDavisBase = true;
			} else
				startDavisBase = false;
		}catch (Exception e) {
			e.printStackTrace();
		}

		if(startDavisBase){
			SystemDescription.splashScreen();

			//Declaring variables to be used 
			String query = null;
			String orgQuery = null;
			boolean flag = true;
			int selecttype = 0;
			
			String sqlPrompt = "davisql> ";
			StringBuilder builder = null;
			Scanner sc = new Scanner(System.in);
			SQLParser parseSQLQuery = null;
			parseSQLQuery = new SQLParser();

			try{
				while(true){
					String inputLine = "";
					builder = new StringBuilder();

					//Gets multi line query inputs from the user
					while(flag){
						System.out.print(sqlPrompt);	
						inputLine = sc.nextLine() + " ";
						builder.append(inputLine);
						if(inputLine.contains(";"))
							flag = false;
					}
					flag = true;
					
					orgQuery = builder.toString().trim().replace(";","");
					orgQuery = orgQuery.replace("'", "").trim();
					
					query = builder.toString().toLowerCase().trim().replace(";","");
					query = query.replace("'", "").trim();
					
					//Parse the inputed query
					parseSQLQuery.parse(query);

					switch(parseSQLQuery.getCurrentQueryType()){
					case ERROR: 
						System.out.println("Oops!! I couldn't understand your SQL syntax!!!");
						break;
					case HELP:
						SystemDescription.showHelp();
						break;
					case SHOW_SCHEMA:
						schemaHandler.showSchema();
						break;
					case USE_SCHEMA:
						schemaHandler.useSchema(orgQuery);
						break;
					case SHOW_TABLES:
						tableHandler.showTables();
						break;
					case CREATE_SCHEMA:
						schemaHandler.createSchema(orgQuery);
						break;
					case CREATE_TABLE:
						createTblFailed = tableHandler.createTable(orgQuery);
						if(!createTblFailed) System.out.println("Query Ok!");
						break;
					case INSERT:
						insertTblFailed = tableHandler.insertTable(orgQuery);
						if(!insertTblFailed) System.out.println("Query Ok!... 1 Row inserted");
						break;
					case SELECT_FROM:
						selecttype = 1;
						tableHandler.selectTable(selecttype, orgQuery);
						break;
					case SELECT_FROM_WHERE:
						selecttype = 2;
						tableHandler.selectTable(selecttype, orgQuery);
						break;
					case SELECT_FROM_IS:
						selecttype = 3;
						tableHandler.selectTable(selecttype, orgQuery);
						break;
					case SELECT_FROM_IS_NOT:
						selecttype = 4;
						tableHandler.selectTable(selecttype, orgQuery);
						break;
					case UPDATE:
						updateTblFailed = tableHandler.updateTable(orgQuery);
						if(!updateTblFailed) System.out.println("Query Ok!... 1 Row ...");
						break;
						
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				sc.close();
			}
		} else {
			System.out.println("Oops!! Problem with information schema... Try Again!.... ");
		}
	}
	
	
}
