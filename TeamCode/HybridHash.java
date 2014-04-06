package edu.buffalo.cse562;

import java.io.File;
import java.util.HashMap;
import java.io.FileWriter;
import java.util.ArrayList;
import java.io.IOException;
import java.io.BufferedWriter;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class HybridHash {

	// this function is used to evaluate the hash join of the tables based on
	// the joining attribute, the return type of the function is the joined table's corresponding Table object
	public static Table evaluateJoin(Table t1, Table t2, String joiningAttribute, File swapDirectory) throws IOException {
		
		// create a file corresponding to the table obtained by joining the tables t1 and t2
		File joinFile = new File(t1.tableDataDirectoryPath.getAbsolutePath() + System.getProperty("file.separator") + (t1.tableName + "|" + t2.tableName + ".tbl"));
		// if the file doesn't exist create the file
		if(!joinFile.exists())
			joinFile.createNewFile();
		
		// this is the Table object corresponding to the table obtained by joining t1 and t2 on the joining attribute
		Table joinedTable = new Table(t1.tableName+"|"+t2.tableName,t1.noOfColumns+t2.noOfColumns,joinFile,t1.tableDataDirectoryPath);
		
		// form the column description list of the joined table by iterating over the column definition lists of t1 and t2
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
		int joiningAttributeIndexTable1 = t1.columnIndexMap.get(t1.tableName + "." + joiningAttribute);
		int joiningAttributeIndexTable2 = t2.columnIndexMap.get(t2.tableName + "." + joiningAttribute); 
		
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
		} else{
			// as the swap parameter is present we divide the data of two tables in buckets which will be placed in the swap directory
			
			// this variable stores the total number of buckets in which the data of the tables will be divided
			int nBuckets = 29;
			
			// divide the data of table t1 into 29 buckets based on java's hashcode function
			for(int i = 0 ; i < nBuckets ; ++i){
				File newBucket = new File(swapDirectory + System.getProperty("file.separator") + t1.tableName+ "bucket" + Integer.toString(i) + ".tbl");
				if(!newBucket.exists())
					newBucket.createNewFile();
			}
			
			String tuple1;
			while((tuple1 = t1.returnTuple()) != null){
				String[] splitTuple = tuple1.split("\\|");
				// this is the bucket number to which we need to place the tuple
				int bucketNumber = (splitTuple[joiningAttributeIndexTable1].hashCode())%nBuckets;
				File bucketPointer = new File(swapDirectory + System.getProperty("file.separator") + t1.tableName+ "bucket" + Integer.toString(bucketNumber) + ".tbl");
				FileWriter fwr = new FileWriter(bucketPointer, true);
				BufferedWriter bwr = new BufferedWriter(fwr);
				bwr.write(tuple1 + "\n");
				bwr.close();
			}
			
			// divide the data of table t2 into 29 buckets based on java's hashcode function
			for(int i = 0 ; i < nBuckets ; ++i){
				File newBucket = new File(swapDirectory + System.getProperty("file.separator") + t2.tableName+ "bucket" + Integer.toString(i) + ".tbl");
				if(!newBucket.exists())
					newBucket.createNewFile();
			}
			
			String tuple2;
			while((tuple2 = t2.returnTuple()) != null){
				String[] splitTuple = tuple2.split("\\|");
				// this is the bucket number to which we need to place the tuple
				int bucketNumber = (splitTuple[joiningAttributeIndexTable1].hashCode())%nBuckets;
				File bucketPointer = new File(swapDirectory + System.getProperty("file.separator") + t2.tableName+ "bucket" + Integer.toString(bucketNumber) + ".tbl");
				FileWriter fwr = new FileWriter(bucketPointer, true);
				BufferedWriter bwr = new BufferedWriter(fwr);
				bwr.write(tuple2 + "\n");
				bwr.close();
			}
		}
		
		// return the joined table formed
		return joinedTable;
	}
}
