package com.utd.dbd.Spring2017;
/**
 * 
 * @author monishaelumalai
 *
 */
public class SystemDescription {
	
	private static String version = "DavisBaseSystem v1.0\n";
	
	public static void splashScreen() { 
		System.out.println();
		System.out.println(line("*",100));
		System.out.println();
		System.out.println("Welcome to DavisBase");
		showVersion();
		showHelp();
		System.out.println(line("*",100));
	}
	
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void showVersion(){
		System.out.println(version);
		System.out.println(line("*",100));
	}

	public static void showHelp() {
		System.out.println(line("*",100));
		System.out.println();
		System.out.println("\tSHOW SCHEMAS\t\t\t:  Displays all the schemas in the Database.");
		System.out.println("\tUSE [SCHEMA_NAME] \t\t:  Selects [SCHEMA_NAME] from the list of schemas.");
		System.out.println("\tSHOW TABLES\t\t\t:  Displays all the tables in the selected schema.");
		System.out.println("\tCREATE [SCHEMA_NAME]\t\t:  Creates [SCHEMA_NAME] in the Database.");
		System.out.println("\tCREATE TABLE [TABLE_NAME]\t:  Creates [TABLE_NAME] in the current schema.");
		System.out.println("\tINSERT INTO [TABLE_NAME]\t:  Inserts values into table.");
		System.out.println("\tSELECT * FROM WHERE\t\t:  Select all values from the table.");
		System.out.println("\tEXIT\t\t\t\t:  Exit the program.");
		System.out.println();
		System.out.println("\tPlease Note:");
		System.out.println("\t\t1. Application is Case Sensitive");
		System.out.println("\t\t2. Supports multi-line inputs");
		System.out.println("\t\t3. Uses ';' as Delimiter");
		System.out.println("\t\t4. All table and index files will be created in the current working directory");
		System.out.println();
		System.out.println(line("*",100));	
	}

}
