package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.IOException;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class HybridHash {

	// this function is used to evaluate the hash join of the tables based on
	// the joining attribute, the return type of the function is the joined table's corresponding Table object
	public static Table evaluateJoin(Table t1, Table t2, String joiningAttribute, File swapDirectory) throws IOException {
		
		// create a file corresponding to the table obtained by joining the tables t1 and t2
		File joinFile = new File(t1.tableDataDirectoryPath.getAbsolutePath() + System.getProperty("file.separator") + (t1.tableName + "|" + t2.tableName + "|Join.tbl"));
		if(!joinFile.exists())
			joinFile.createNewFile();
		
		// this is the Table object corresponding to the table obtained by joining t1 and t2 on the joining attribute
		Table joinedTable = new Table(t1.tableName+"|"+t2.tableName,t1.noOfColumns+t2.noOfColumns,joinFile,t1.tableDataDirectoryPath);
		
		// form the column description list of the joined table
		ArrayList<ColumnDefinition> joinedTableColumnDefinitionList = new ArrayList<ColumnDefinition>();
		for(ColumnDefinition cd : t1.columnDescriptionList){
			ColumnDefinition temp = new ColumnDefinition();
			temp.setColumnName(cd.getColumnName());
			temp.setColDataType(cd.getColDataType());
			joinedTableColumnDefinitionList.add(temp);
		}
		for(ColumnDefinition cd : t2.columnDescriptionList){
			ColumnDefinition temp = new ColumnDefinition();
			temp.setColumnName(cd.getColumnName());
			temp.setColDataType(cd.getColDataType());
			joinedTableColumnDefinitionList.add(temp);
		}
		
		joinedTable.columnDescriptionList = joinedTableColumnDefinitionList;
		
		// populate the column index map of the joined table
		joinedTable.populateColumnIndexMap();
		
		// get the indexes of the joining attribute in table1 and table2 to index the tuples in the correct manner
		int joiningAttributeIndexTable1 = t1.columnIndexMap.get(joiningAttribute);
		int joiningAttributeIndexTable2 = t2.columnIndexMap.get(joiningAttribute); 
		
		// if the swap directory is a null value in that case everything needs to be handled in memory
		if(swapDirectory == null){
			// this is the file pointer for table1
			File tableFile1 = t1.tableFilePath;
			// this is the file pointer for table2
			File tableFile2 = t2.tableFilePath;
			// this HashMap< String, ArrayList<String> > is interpreted as follows, HashMap<joinAttribute, List of Strings that contain that attribute>
			HashMap<String, ArrayList<String> > hashJoinTable = new HashMap<String, ArrayList<String>>();
			
			// if the size of the first table is greater than the size of the other table then we store the other table in the HashMap
			if(tableFile1.length() > tableFile2.length()){
				
				// scan the strings in the tableFile2 and put them into HashMap
				String tupleString;
				while((tupleString = t2.returnTuple()) != null){
					// this string array stores different components of a tuple
					String[] tupleComponents = tupleString.split("\\|");
					if(hashJoinTable.containsKey(tupleComponents[joiningAttributeIndexTable2]))
						hashJoinTable.get(tupleComponents[joiningAttributeIndexTable2]).add(tupleString);
					else{
						// make a new ArrayList of String objects which would hold the tuples that have same value for the key attribute
						ArrayList<String> newTupleStringsList = new ArrayList<String>();
						newTupleStringsList.add(tupleString);
						hashJoinTable.put(tupleComponents[joiningAttributeIndexTable2], newTupleStringsList);
					}
				}
				
				// form the join by reading table1 tuple by tuple and probing the HashMap that corresponds to Table2
				while((tupleString = t1.returnTuple()) != null){
					// this string array stores different components of a tuple
					String[] tupleComponents = tupleString.split("\\|");
					
					// now probe the hash table to form the join
					if(hashJoinTable.containsKey(tupleComponents[joiningAttributeIndexTable1])){
						// allocate a new BufferedWriter object to write to the joinedTable file
						BufferedWriter bwr = new BufferedWriter(new FileWriter(joinedTable.tableFilePath, true));
						// get the list of strings to join the tuple with
						ArrayList<String> joiningTuples = hashJoinTable.get(tupleComponents[joiningAttributeIndexTable1]);
						// perform the join operation
						for(String joinString : joiningTuples){
							bwr.write(tupleString + joinString + "\n");
						}
						bwr.close();
					}
				}
				
			} else{
				
				// scan the strings in the tableFile1 and put them into HashMap as tableFile1 is smaller in length
				String tupleString;
				while((tupleString = t1.returnTuple()) != null){
					// this string array stores different components of a tuple
					String[] tupleComponents = tupleString.split("\\|");
					if(hashJoinTable.containsKey(tupleComponents[joiningAttributeIndexTable1]))
						hashJoinTable.get(tupleComponents[joiningAttributeIndexTable1]).add(tupleString);
					else{
						// make a new ArrayList of String objects which would hold the tuples that have same value for the key attribute
						ArrayList<String> newTupleStringsList = new ArrayList<String>();
						newTupleStringsList.add(tupleString);
						hashJoinTable.put(tupleComponents[joiningAttributeIndexTable1], newTupleStringsList);
					}
				}
				
				// form the join by reading table2 tuple by tuple and probing the HashMap that corresponds to Table1
				while((tupleString = t2.returnTuple()) != null){
					// this string array stores different components of a tuple
					String[] tupleComponents = tupleString.split("\\|");
					
					// now probe the hash table to form the join
					if(hashJoinTable.containsKey(tupleComponents[joiningAttributeIndexTable2])){
						// allocate a new BufferedWriter object to write to the joinedTable file
						BufferedWriter bwr = new BufferedWriter(new FileWriter(joinedTable.tableFilePath, true));
						// get the list of strings to join the tuple with
						ArrayList<String> joiningTuples = hashJoinTable.get(tupleComponents[joiningAttributeIndexTable2]);
						// perform the join operation
						for(String joinString : joiningTuples){
							bwr.write(joinString + tupleString + "\n");
						}
						bwr.close();
					}
				}
			}
		}

	}
}
