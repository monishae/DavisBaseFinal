package com.utd.dbd.Spring2017;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
/**
 * 
 * @author monishaelumalai
 *
 */
public class SQLParser {


	private String showSchemaStr = null;
	private String useStr = null;
	private String showTablesStr = null;
	private String createSchemaStr = null;
	private String createTableStr = null;
	private String selectFromWhereIsStr = null;
	private String selectFromWhereNotStr = null;
	private String selectFromWhereStr = null;
	private String selectFromStr = null;
	private String insertStr = null;
	private String exitStr = null;
	private String version =null;
	private String helpStr = null;
	public QueryType currentQueryType;
	
	private Pattern selectFromWhereIsPat, selectFromWhereNotPat, selectFromWherePat, selectFromPat, insertPat,exitPat,helpPat,versionPat;
	private Pattern	showSchemaPat,usePat,showTablePat, createSchemaPat,createTablePat;

	
	public enum QueryType{
		ERROR,
		SHOW_SCHEMA,
		USE_SCHEMA,
		SHOW_TABLES,
		CREATE_SCHEMA,
		CREATE_TABLE,
		INSERT,
		SELECT_FROM_IS,
		SELECT_FROM_IS_NOT,
		SELECT_FROM_WHERE,
		SELECT_FROM, UPDATE,HELP,VERSION
	};
	
	
	public void setCurrentQueryType(QueryType current){
		this.currentQueryType = current;
	}
	
	
	public QueryType getCurrentQueryType(){
		return this.currentQueryType;
	}
	
	public SQLParser(){

		this.showSchemaStr = "SHOW\\s+?SCHEMAS";
		this.useStr = "USE\\s+?[^\\s]+";
		this.showTablesStr = "SHOW\\s+TABLES";
		this.createSchemaStr = "CREATE\\s+?SCHEMA\\s+?[^\\s]+";
		this.createTableStr = "CREATE\\s+?TABLE\\s+?(\\w)+\\s*\\(\\s*?([\\w\\,\\s*\\.\\(\\)0-9]|[\\w\\s*\\.\\(\\)0-9])+?\\s*\\)"; 
		this.selectFromWhereIsStr = "SELECT\\s+?[*]\\s+?FROM\\s+?[^\\s]+?\\s+?(WHERE.*\\s+?[^\\s]+?\\s+?IS\\s+?NULL+)";
		this.selectFromWhereNotStr = "SELECT\\s+?[*]\\s+?FROM\\s+?[^\\s]+?\\s+?(WHERE.*\\s+?[^\\s]+?\\s+?IS\\s+?NOT\\s+?NULL)";
		this.selectFromWhereStr = "SELECT\\s+?[*]\\s+?FROM\\s+?[^\\s]+?\\s+?(WHERE.*\\s+?[^\\s]+?\\s*?[<|>|=|>=|<=]\\s*?[^\\s]+)";
		this.selectFromStr = "SELECT\\s+?[*]\\s+?FROM\\s+?[^\\s]+";
		this.insertStr = "INSERT\\s+?INTO\\s+?[^\\s]+?\\s+?VALUES\\s*?\\(\\s?([\\w\\,\\s*\\-\\'\\.]|[\\w])+?\\)";
		this.version = "version\\s*?";
		this.helpStr ="help\\s*?";
		this.exitStr = "exit\\s*?";

		initializePattern();
	}

	private void initializePattern(){
		this.showSchemaPat = Pattern.compile(this.showSchemaStr, Pattern.MULTILINE | Pattern.DOTALL |Pattern.CASE_INSENSITIVE);
		this.usePat = Pattern.compile(this.useStr, Pattern.MULTILINE | Pattern.DOTALL |Pattern.CASE_INSENSITIVE);
		this.showTablePat = Pattern.compile(this.showTablesStr,Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.createSchemaPat = Pattern.compile(this.createSchemaStr,Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.createTablePat = Pattern.compile(this.createTableStr,Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.selectFromWhereIsPat = Pattern.compile(this.selectFromWhereIsStr, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.selectFromWhereNotPat = Pattern.compile(this.selectFromWhereNotStr, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.selectFromWherePat = Pattern.compile(this.selectFromWhereStr, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.selectFromPat = Pattern.compile(this.selectFromStr, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.insertPat = Pattern.compile(this.insertStr, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.exitPat = Pattern.compile(this.exitStr, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.helpPat = Pattern.compile(this.helpStr, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
		this.versionPat = Pattern.compile(this.version, Pattern.MULTILINE | Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
	}

	
	public void parse(String query){
		Matcher matcher = this.selectFromWhereIsPat.matcher(query);
		
		try{
			if(this.exitPat.matcher(query).matches()){
				System.out.println("Exiting DavisBase...Bye...");
				System.exit(0);
			}else if(this.showSchemaPat.matcher(query).matches()){ 
				setCurrentQueryType(QueryType.SHOW_SCHEMA);  	
			}else if(this.usePat.matcher(query).matches()){
				setCurrentQueryType(QueryType.USE_SCHEMA);  
			}else if(this.showTablePat.matcher(query).matches()){ 
				setCurrentQueryType(QueryType.SHOW_TABLES); 	
			}else if(this.createSchemaPat.matcher(query).matches()){ 
				setCurrentQueryType(QueryType.CREATE_SCHEMA); 	
			}else if(this.createTablePat.matcher(query).matches()){ 
				setCurrentQueryType(QueryType.CREATE_TABLE); 
			}else if(matcher.matches()){
				setCurrentQueryType(QueryType.SELECT_FROM_IS); 
			}else if(this.selectFromWhereNotPat.matcher(query).matches()){
				setCurrentQueryType(QueryType.SELECT_FROM_IS_NOT); 
			}else if(this.selectFromWherePat.matcher(query).matches()){
				setCurrentQueryType(QueryType.SELECT_FROM_WHERE); 
			}else if(this.selectFromPat.matcher(query).matches()){
				setCurrentQueryType(QueryType.SELECT_FROM); 
			}else if(this.insertPat.matcher(query).matches()){
				setCurrentQueryType(QueryType.INSERT); 
			}else if(this.helpPat.matcher(query).matches()){
				setCurrentQueryType(QueryType.HELP); 
			}else{
				setCurrentQueryType(QueryType.ERROR); 
			}
		}catch(PatternSyntaxException pe){
			System.out.println("Error in Pattern Syntax!!!");
			pe.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
