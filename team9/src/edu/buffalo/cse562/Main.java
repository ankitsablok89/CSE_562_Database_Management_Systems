package edu.buffalo.cse562;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

/* this class represents the Column_Name and Type pair */
@SuppressWarnings("unused")
class Pair{
	
	// this is the name of the column
	String columnName;
	// this is the type of the column
	String columnTypeName;
	
	/* this constructor is used to create a copy of a pair object given as a parameter */
	public Pair(Pair pairToCopy){
		this.columnName = pairToCopy.columnName;
		this.columnTypeName = pairToCopy.columnTypeName;
	}
	
	/* this is the constructor for the pair class */
	public Pair(String columnName , String columnTypeName){
		this.columnName = columnName;
		this.columnTypeName = columnTypeName;
	}
}

/* this class represents the tables that we use in the SQL query evaluation */
class Table{
	
	// this is the name of the table
	String tableName;
	// this is the number of columns in the tables
	int noOfColumns;
	// this is the list of columns
	ArrayList<Pair> columnDescription;
	// this HashMap is used to map column names to respective indices in the table
	HashMap<String, Integer> columnIndexMap;
	// this variable is used to store the path of the .dat file to which the table points in the given directory
	String tableDatFilePath;
	// this variable stores the directory location where all the .dat files are stored, in-fact this is the parent directory of the table
	String dataDirectoryPath;
	// this is the FileReader object for the table
	FileReader fr = null;
	// this is the BufferedReader object for the table
	BufferedReader br = null;
	// this is the ArrayList of strings describing the table
	ArrayList<String> tableString = new ArrayList<String>();
	
	/* this constructor is used to create a new table which is a replica of the table given as input */
	public Table(String tableName , Table tableToCopy) throws IOException{
		
		this.tableName = tableName;
		this.noOfColumns = tableToCopy.noOfColumns;
		this.columnDescription = tableToCopy.columnDescription;
		this.columnIndexMap = tableToCopy.columnIndexMap;
		this.tableDatFilePath = tableToCopy.tableDatFilePath;
		this.dataDirectoryPath = tableToCopy.dataDirectoryPath;
		if((new File(this.tableDatFilePath)).exists())
			this.populateTableList();
	}
	
	/* this is the constructor for the table class */
	public Table(String tableName, String dataDirectoryPath, String tableDatFilePath, int noOfColumns) throws IOException{
		
		this.tableName = tableName;
		this.noOfColumns = noOfColumns;
		this.columnDescription = new ArrayList<Pair>();
		this.tableDatFilePath = tableDatFilePath;
		this.dataDirectoryPath = dataDirectoryPath;
		this.columnIndexMap = new HashMap<String, Integer>();
		if((new File(this.tableDatFilePath)).exists())
			this.populateTableList();
	}
	
	/* this function is used to allocate the BufferedReader and the FileReader object for the .dat file associated with the table */
	public void allocateBufferedReaderForTableFile() throws FileNotFoundException{
		this.fr = new FileReader(new File(this.tableDatFilePath));
		this.br = new BufferedReader(fr,1000);
	}
	
	/* this function is used to populate the columnIndexMap of the table */
	public void populateColumnIndexMap(){
		
		// this is the initial column index
		int colIndex = 0;
		for(Pair pairIterator : columnDescription){
			this.columnIndexMap.put(pairIterator.columnName, colIndex);
			++colIndex;
		}
	}
	
	/* this function is used to return a tuple at a time from the .dat file describing the table */
	public String returnTuple() throws IOException{
		
		// if both of these are null then allocate the reader object for the file corresponding the table
		if(this.br == null && this.fr == null)
			this.allocateBufferedReaderForTableFile();
		
		// this string is used to read a '|' separated tuple of the .dat file
		//StringBuilder scanString = new StringBuilder("");
		String scanString = null;
		// read the string using the BufferedReader
		//scanString.append(br.readLine())/* = br.readLine()*/;
		scanString = br.readLine();
		
		// if the scanned string contains a '|' as the end character
		if(scanString != null ){
			if(scanString.length()>1){
			if(scanString.charAt(scanString.length() - 1) == '|'){
				//scanString.append(scanString.substring(0, scanString.length()-1));// = scanString.substring(0, scanString.length() - 1);
			//scanString = new StringBuilder(scanString.substring(0, scanString.length()-1));
			scanString = scanString.substring(0, scanString.length()-1);
			}
			}
		}
		
		//if(scanString.toString() == ""){
		if(scanString == null){
			br.close();
			br = null;
			fr = null;
			//System.gc();
		}
		
		//return scanString.toString();
		return scanString;
		
	}
	
	/* this function is used to populate the list of strings describing the table */
	public void populateTableList() throws IOException{
		
		String scanString;
		while((scanString = returnTuple()) != null)
			this.tableString.add(scanString);
	}
	
	/* this function is used to print the table - tuple by tuple */
	public void printTuples() throws IOException{
		
		// allocate a new FileReader and a BufferedReader object to read through the table's file
		FileReader fr = new FileReader(new File(this.tableDatFilePath));
		BufferedReader br = new BufferedReader(fr);
		
		String scanString;
		int nRows = 0;
		while((scanString = br.readLine()) != null){
			System.out.println(scanString);
			++nRows;
		}
	}
	
}

public class Main {
	
	// this HashMap is used to store the link between the name of the table and the corresponding .dat file in the --data directory
	public static HashMap<String, String> tableLinks;
	
	// this HashMap stores the link between the table name and the corresponding table object
	public static HashMap<String, Table> tableMap;
	
	// this is the directory location where all the .dat files are stored
	public static File dataDirectory = null;
	
	// this is the array list that stores all the .sql files input from the command prompt
	public static ArrayList<File> sqlFiles = new ArrayList<File>();
	
	@SuppressWarnings("unused")
	public static void main(String[] args) throws ParseException, IOException {

		/* Step - 1 : Scan the command line arguments and store all the SQL files and the data directory where all the .dat files are stored */
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("--data")) {
				dataDirectory = new File(args[i + 1]);
				++i;
			} else {
				sqlFiles.add(new File(args[i]));
			}
		}
		
		/* Step - 2 : Associate all the tables in the data directory with their absolute paths HashMap<TableName, File_Path> */
		tableLinks = new HashMap<String, String>();
		
		if(dataDirectory != null && dataDirectory.isDirectory()){
			for(File file : dataDirectory.listFiles()){
				if(file.getName().endsWith("dat") || file.getName().endsWith("tbl")){
					// store the name of the file without extension
					tableLinks.put(file.getName().trim().substring(0,file.getName().lastIndexOf(".")).toLowerCase(),file.toString());
				}
			}
		}
		
		/* Step - 3 : Now we start parsing the commands that we receive from the SQL files */
		for(File sqlCommandsFile :sqlFiles){
			
			// firstly get the names of the tables the queries operate on
			FileReader fr = new FileReader(sqlCommandsFile);
			CCJSqlParser parserObject = new CCJSqlParser(fr);
			
			// this is the HashMap<table_name,Table> objects
			tableMap = new HashMap<String, Table>();
			
			// this statement object is used to read of the statements from the SQL file
			Statement stmtObject;
			
			while( (stmtObject = parserObject.Statement()) != null ){
				
				// check if the statement is a "CREATE TABLE" statement
				if(stmtObject instanceof CreateTable){

					// create a new table object, pass in the number of columns in the constructor
					Table newTable = new Table(((CreateTable) stmtObject).getTable().toString().toLowerCase(),dataDirectory.getAbsolutePath(), tableLinks.get(((CreateTable) stmtObject).getTable().toString().toLowerCase()) ,((CreateTable) stmtObject).getColumnDefinitions().size());
					
					// put this table in the map of tables
					tableMap.put(newTable.tableName.toLowerCase(), newTable);
					
					// form the column description list of the table
					for(Object columnName : ((CreateTable) stmtObject).getColumnDefinitions()){
						
						// this string consists of the column name and type both
						String tempString = columnName.toString();
						String[] columnNameAndType = tempString.split(" ");
						
						// make a new pair object
						Pair newColumnTypePair = new Pair(columnNameAndType[0] , columnNameAndType[1]);
						// add the new column pair to the list of column,type pairs
						newTable.columnDescription.add(newColumnTypePair);
					}
					
					// populate the columnIndexMap of the newly formed table object
					newTable.populateColumnIndexMap();
					
				}
				
				// check if the statement is a "SELECT" statement
				if(stmtObject instanceof Select){
					
					// this statement is used to get the select statement's body
					SelectBody selectBody = ((Select) stmtObject).getSelectBody();
					
					// this statement is used to get the list of tables for which we need to compute the Cartesian product
					ArrayList<String> tablesForCartesianProductStrings = new ArrayList<String>();
					
					if(((PlainSelect)selectBody).getFromItem() != null){
						if(((PlainSelect)selectBody).getFromItem().toString().replaceAll("\\(", "").replaceAll("\\)", "").contains("SELECT"))
							tablesForCartesianProductStrings.add(((PlainSelect)selectBody).getFromItem().toString().replaceAll("\\(", "").replaceAll("\\)", ""));
						else
							tablesForCartesianProductStrings.add(((PlainSelect)selectBody).getFromItem().toString().replaceAll("\\(", "").replaceAll("\\)", "").toLowerCase());
					}
					
					if(((PlainSelect)selectBody).getJoins() != null){
						for(Object join : ((PlainSelect)selectBody).getJoins()){
							if(join.toString().replaceAll("\\(", "").replaceAll("\\)", "").contains("SELECT"))
								tablesForCartesianProductStrings.add(join.toString().replaceAll("\\(", "").replaceAll("\\)", ""));
							else
								tablesForCartesianProductStrings.add(join.toString().replaceAll("\\(", "").replaceAll("\\)", "").toLowerCase());
						}
					}
					
					// from the above list of strings describing tables we now create a list of actual tables for which we need to evaluate the Cartesian product
					ArrayList<Table> tablesForCartesianProduct = new ArrayList<Table>();
					
					for(String tableString : tablesForCartesianProductStrings){
						
						// this is the new table object to be added to the list of table for evaluating the Cartesian product
						Table newTable = null;
						
						if(tableString.contains("select") || tableString.contains("SELECT")){
							tablesForCartesianProduct.add(SubQueryEvaluator.evaluateSubQuery(tableString));
						}
						// if the table string contains an AS then that mean it has an "ALIAS"
						else if(tableString.contains("AS") || tableString.contains("as")){
							
							String[] tableStringTokens = tableString.split("\\s+");
							newTable = new Table(tableStringTokens[2].toLowerCase(), tableMap.get(tableStringTokens[0]));
							
							tablesForCartesianProduct.add(newTable);
							
						} else{
							tablesForCartesianProduct.add(tableMap.get(tableString));
						}
					}
					
					// store the Cartesian product of the tables in a new table object
					Table cartesianProductTable = CartesianProduct.returnCartesianProduct(tablesForCartesianProduct);
					
					// this is the resultant table we obtain after applying the WHERE clause to the table obtained above
					Table resultTable = null;
					
					// selectionExpression extracts the WHERE condition from the SELECT query and applies it to the Cartesian product of the tables obtained above
					Expression selectionExpression=null;
					if((selectionExpression = (Expression) ((PlainSelect)selectBody).getWhere()) != null){
						resultTable = SelectionOperation.selectionOnTable(selectionExpression,cartesianProductTable);
						//System.out.println(resultTable);
					}else{
						resultTable = cartesianProductTable;
					}
					
					@SuppressWarnings("rawtypes")
					List selectItemList = ((PlainSelect)selectBody).getSelectItems();
					ArrayList<String> selectList = new ArrayList<String>();
					for(Object column : selectItemList){
						selectList.add(column.toString());
					}
	
					// this is the list of group by column references in the select query
					@SuppressWarnings("rawtypes")
					List groupByColumns = ((PlainSelect)selectBody).getGroupByColumnReferences();
					
					// this variable is used to check if there is an aggregate function present in the select list
					boolean aggregatePresent = false;
					for(String selectItem : selectList){
						if(selectItem.contains("sum(") || selectItem.contains("SUM(") || selectItem.contains("avg(") || selectItem.contains("AVG(") || selectItem.contains("count(") || selectItem.contains("COUNT(")){
							aggregatePresent = true;
							break;
						}
					}
					
					List orderByList = ((PlainSelect)selectBody).getOrderByElements();
					if(groupByColumns == null && !aggregatePresent){
						Table table = ProjectTableOperation.projectTable(resultTable, selectList);//.printTuples();
						
						if(orderByList != null){
							table = OrderByOperation.orderBy(table, orderByList, orderByList.size());
							
						}
						table.printTuples();
						
					}else{
						
						// make the object of Aggregate class to call Aggregate functions
						Aggregate aggrObject = new Aggregate();
						
						String groupItems;
						
						// this is the string of group by items
						if(groupByColumns == null)
							groupItems = "NOGroupBy";
						else
							groupItems = groupByColumns.toString();
						
						// this is the array of select items
						String[] selectItemsArray = ((PlainSelect)selectBody).getSelectItems().toString().replaceAll("\\[", "").replaceAll("\\]", "").trim().split(",");
						
						// call the aggregate function
						aggrObject.getSum(resultTable, selectItemsArray, groupItems,orderByList);
					}
				}
			}
					
		}
			
	}
}

