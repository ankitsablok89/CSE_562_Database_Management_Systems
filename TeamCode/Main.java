package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

/* this class is used to create an object that consists of the name of the attribute and the type of the column */
class Pair{
	
	// this variable stores the attribute name
	String columnName;
	// this variable stores the type of the attribute
	String columnType;
	
	// this constructor is used to form a new Pair object
	public Pair(String columnName , String columnType){
		this.columnName = columnName;
		this.columnType = columnType;
	}
	
	// this constructor is used to create a clone Pair object
	public Pair(Pair pairToClone){
		this.columnName = pairToClone.columnName;
		this.columnType = pairToClone.columnType;
	}
}

/* this is the class that we use to form Table objects which consist of various attributes of tables */
class Table{
	
	// this is the name of the table
	String tableName;
	// this is the number of columns in the tables
	int noOfColumns;
	// this File object stores the path to the table file
	File tableFilePath;
	// this File object stores the directory in which the table is present
	File tableDataDirectory;
	// this is the list of columnName and columnType pairs corresponding to the table
	ArrayList<Pair> columnDescription;
	// this HashMap stores the mappings from the columnNames to integer indices in the table
	HashMap<String, Integer> columnIndexMap;
	
}

/* this is the Main class for the project */
public class Main {
	public static void main(String[] args) {
		
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
			String fileName = tableFile.getName();
			
			if(fileName.endsWith(".tbl") || fileName.endsWith(".dat")){
				if(!tablesNameAndFileMap.containsKey(fileName.substring(0, fileName.lastIndexOf(".")))){
					tablesNameAndFileMap.put(fileName.substring(0, fileName.lastIndexOf(".")), tableFile);
				}
			}
		}
		
		
		
	}
}