# Database Engine 

A rudimentary database engine implementation that is loosely based on MySQL. 


## Requirements
Java 8.

### Prompt 

Upon launch, the application should present a prompt, where interactive
commands may be entered.

davisql>

### Supported Commands (Summary)

Your database engine must support the following high-level commands. All commands should be terminated by a semicolon (;).

 * SHOW SCHEMAS – Displays all schemas defined in your database.
 * USE – Chooses a schema.
 * SHOW TABLES – Displays all tables in the currently chosen schema.
 * CREATE SCHEMA – Creates a new schema to hold tables.
 * CREATE TABLE – Creates a new table schema, i.e. a new empty table.
 * INSERT INTO TABLE – Inserts a row/record into a table.
 * SELECT-FROM-WHERE - style query
 * EXIT – Cleanly exits the program and saves all table and index information in non-volatile files.

#### SHOW SCHEMAS;

Display a list of all database schemas by name, including the system information_schema.

##### USE schema_name;

This command checks if the schema is present and if present use the schema for further actions.

##### SHOW TABLES;

Show a list all table names in the currently used schema.

##### CREATE SCHEMA schema_name;

Create a new schema.

##### CREATE TABLE

	**CREATE TABLE** table_name (
	column_name1 data_type(size) [primary key|not null],
	column_name2 data_type(size) [primary key|not null],
	column_name3 data_type(size) [primary key|not null],
	...
	);
example: 
    CREATE TABLE Students(ID INT(4) PRIMARY KEY,NAME VARCHAR(25),BDATE DATE(8),CREDITS SHORT(2));

Create the table schema information for a new table. It will be created in the current schema. In other words, add appropriate entries to the system information_schema tables that define the described **CREATE TABLE**.

Your table definition should support the following data types. All numbers should be represented as bytes in _Big Endian_ order.

| Datatype  | Data size (bytes) | 
| ----------| ------------------| 
| BYTE 		| 1 				|
| SHORT 	| 2 				|
| INT 		| 4 				|
| LONG		| 8					|
| CHAR(n) 	| n					|
| VARCHAR(n)| any n value		|
| FLOAT 	| 4 				|
| DOUBLE 	| 8 				|
| DATETIME 	| 8					|
| DATE 		| 8 				|

The only table constraints that are support are PRIMARY KEY and NOT NULL (to indicate that NULL values are not permitted for a particular column). All primary keys are single column keys. If a column is a primary key, its **information_schema.COLUMNS.COLUMN_KEY** attribute will be **“PRI”**, otherwise, it will be the empty string. If a column is defined as **NOT NULL**, then its **information_schema.COLUMNS.IS_NULLABLE** attribute will be **“NO”**, otherwise, it will be **“YES”**. Base does not support **FOREIGN KEY**.

##### INSERT INTO TABLE

	INSERT INTO TABLE table_name VALUES (value1,value2,value3,…);

example:
 INSERT INTO TABLE Students VALUES (1,'Mito Are','1995-08-21',32);

Insert a new record into the selected table. If n values are supplied, they will be mapped onto the first n columns. 

##### SELECT-FROM-WHERE

	SELECT *
	FROM table_name
	WHERE column_name operator value;

SELECT * FROM table_name;

### EXIT
    Exits from the application.
	
