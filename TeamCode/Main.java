package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.io.FileReader;
import java.util.ArrayList;
import java.io.FileNotFoundException;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

/* this is the class that we use to form Table objects which consist of various attributes of tables */
class Table{
	
	// this is the name of the table
	String tableName;
	// this is the number of columns in the tables
	int noOfColumns;
	// this File object stores the path to the table file
	File tableFilePath;
	// this File object stores the directory in which the table is present
	File tableDataDirectoryPath;
	// this is the list of columnName and columnType pairs corresponding to the table
	ArrayList<ColumnDefinition> columnDescriptionList;
	// this HashMap stores the mappings from the columnNames to integer indices in the table
	HashMap<String, Integer> columnIndexMap;
	
	// this is the constructor for the Table class
	public Table(String tableName, int noOfColumns, File tableFilePath, File tableDataDirectoryPath){
		this.tableName = tableName;
		this.noOfColumns = noOfColumns;
		this.tableFilePath = tableFilePath;
		this.tableDataDirectoryPath = tableDataDirectoryPath;
		this.columnDescriptionList = new ArrayList<ColumnDefinition>();
		this.columnIndexMap = new HashMap<String, Integer>();
	}
	
	// this function is used to make a clone of the table given as input
	public Table(Table tableToClone){
		this.tableName = tableToClone.tableName;
		this.noOfColumns = tableToClone.noOfColumns;
		this.tableFilePath = tableToClone.tableFilePath;
		this.tableDataDirectoryPath = tableToClone.tableDataDirectoryPath;
		this.columnDescriptionList = tableToClone.columnDescriptionList;
		this.columnIndexMap = tableToClone.columnIndexMap;
	}
	
	// this function is used to read the tuples of the table
	public void readTable() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(tableFilePath));
		String bufferString;
		while((bufferString = br.readLine()) != null)
			System.out.println(bufferString);
	}
	
	// this function is used to populate the columnIndexMap for the table
	public void populateColumnIndexMap(){
		int indexCounter = 0;
		for(ColumnDefinition dummyPair : this.columnDescriptionList){
			this.columnIndexMap.put(dummyPair.getColumnName(), indexCounter++);
		}
	}
}

/* this is the Main class for the project */
public class Main {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws FileNotFoundException, ParseException {
		
		// this File object is the one that points to the data directory, this directory consists of all the 
		// .dat or the .tbl files in which our data is stored
		File dataDirectory = null;
		
		// this File object is the one that points to the swap directory, this is the directory to which we can
		// write during the course of our project execution
		File swapDirectory = null;
		
		// this is the ArrayList that stores the File objects corresponding to all the .sql files supplied on input
		ArrayList<File> sqlFileList = new ArrayList<File>();
		
		// this HashMap stores the (table_name, table_file_path) pairs so that the look up for the tables becomes easy
		HashMap<String, File> tablesNameAndFileMap = new HashMap<String, File>();
		
		// iterate over the args[] array and set the above variables
		for(int i = 0 ; i < args.length ; ++i){
			
			if(args[i].equals("--data")){
				dataDirectory = new File(args[i+1]);
				++i;
			} else if(args[i].equals("--swap")){
				swapDirectory = new File(args[i+1]);
				++i;
			} else{
				sqlFileList.add(new File(args[i]));
			}
		}
		
		// after setting all the variables populate the HashMap with (table_name, table_file_path) pairs
		for(File tableFile : dataDirectory.listFiles()){
			// this is the actual name of the file
			String fileName = tableFile.getName().toLowerCase();
			
			if(fileName.endsWith(".tbl") || fileName.endsWith(".dat")){
				if(!tablesNameAndFileMap.containsKey(fileName.substring(0, fileName.lastIndexOf(".")))){
					tablesNameAndFileMap.put(fileName.substring(0, fileName.lastIndexOf(".")), tableFile);
				}
			}
		}
		
		// start scanning the .sql files provided in the input
		for(File sqlFile : sqlFileList){
			
			// this is a FileReader object used in parsing the SQL file
			FileReader fr = new FileReader(sqlFile);
			CCJSqlParser parserObject = new CCJSqlParser(fr);
			
			// this HashMap stores the (table_name, table_TableObject) pairs, which is used to get the referenced Table object directly given a table's name
			HashMap<String, Table> tableObjectsMap = new HashMap<String, Table>();
			
			// this is the statement object returned by the CCJSqlParser when scanning the sql file
			Statement statementObject;
			
			while((statementObject = parserObject.Statement()) != null){
				
				// check if the statement just scanned is an instance of the CreateTable statement
				if(statementObject instanceof CreateTable){
					
					// make a new Table object corresponding to the CreateTable statement encountered
					CreateTable ctStmt = (CreateTable)statementObject;
					
					// this String object stores the name of the table
					String tableName = ctStmt.getTable().getName();
					
					// this Table is a reference to the table that is present inside the create table statement
					Table newTableObject = new Table(tableName,ctStmt.getColumnDefinitions().size(),tablesNameAndFileMap.get(tableName), dataDirectory);
					
					// set the attributes of this new table object
					newTableObject.columnDescriptionList = (ArrayList<ColumnDefinition>) ctStmt.getColumnDefinitions();
					
					// populate the column index map of the table object
					newTableObject.populateColumnIndexMap();
					
					// insert the pair of (table_name, table_TableObject) in the tableObjectsMap
					tableObjectsMap.put(tableName, newTableObject);
				} else if(statementObject instanceof Select){
					
				}
			}
		}
		
	}
}