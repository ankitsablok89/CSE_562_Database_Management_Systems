package edu.buffalo.cse562;

import java.io.File;
import java.util.HashMap;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class HybridHash {

	// this function is used to evaluate the hash join of the tables based on
	// the joining attribute, the return type of the function is the joined table's corresponding Table object
	public static Table evaluateJoin(Table t1, Table t2, String t1Name, String t2Name, String joiningAttribute, File swapDirectory) throws IOException {
		
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
		int joiningAttributeIndexTable1 = t1.columnIndexMap.get(t1Name + "." + joiningAttribute);
		int joiningAttributeIndexTable2 = t2.columnIndexMap.get(t2Name + "." + joiningAttribute); 
		
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
			int nBuckets = 103;
			
			// divide the data of table t1 into 31 buckets based on java's HashCode function
			// this is the ArrayList of file objects corresponding to table1's buckets
			ArrayList<File> table1BucketsFileObjects = new ArrayList<File>();
			for(int i = 0 ; i < nBuckets ; ++i){
				File newBucket = new File(swapDirectory + System.getProperty("file.separator") + t1.tableName + "bucket" + Integer.toString(i) + ".tbl");
				table1BucketsFileObjects.add(newBucket);
				if(!newBucket.exists())
					newBucket.createNewFile();
			}
			
			String tuple1;
			while((tuple1 = t1.returnTuple()) != null){
				String[] splitTuple = tuple1.split("\\|");
				// this is the bucket number to which we need to place the tuple
				int tupleHashCode = splitTuple[joiningAttributeIndexTable1].hashCode();
				// if the hash code is negative then multiply it by -1
				if(tupleHashCode < 0)
					tupleHashCode = tupleHashCode*-1;
				// this is the bucket number to which we need to place the tuple
				int bucketNumber = (tupleHashCode)%nBuckets;
				File bucketPointer = table1BucketsFileObjects.get(bucketNumber);
				FileWriter fwr = new FileWriter(bucketPointer, true);
				BufferedWriter bwr = new BufferedWriter(fwr);
				bwr.write(tuple1 + "\n");
				bwr.close();
			}
			
			// divide the data of table t2 into 31 buckets based on java's HashCode function
			// this is the ArrayList of file objects corresponding to table2's buckets
			ArrayList<File> table2BucketsFileObjects = new ArrayList<File>();
			for(int i = 0 ; i < nBuckets ; ++i){
				File newBucket = new File(swapDirectory + System.getProperty("file.separator") + t2.tableName+ "bucket" + Integer.toString(i) + ".tbl");
				table2BucketsFileObjects.add(newBucket);
				if(!newBucket.exists())
					newBucket.createNewFile();
			}
			
			String tuple2;
			while((tuple2 = t2.returnTuple()) != null){
				String[] splitTuple = tuple2.split("\\|");
				// this is the bucket number to which we need to place the tuple
				int tupleHashCode = splitTuple[joiningAttributeIndexTable2].hashCode();
				// if the tuple hashcode is negative then multiply it by -1
				if(tupleHashCode < 0)
					tupleHashCode = -1*tupleHashCode;
				int bucketNumber = (tupleHashCode)%nBuckets;
				File bucketPointer = table2BucketsFileObjects.get(bucketNumber);
				FileWriter fwr = new FileWriter(bucketPointer, true);
				BufferedWriter bwr = new BufferedWriter(fwr);
				bwr.write(tuple2 + "\n");
				bwr.close();
			}
			
			// now evaluate the hash join of the the buckets to form the complete join of the tables
			// To evaluate the hybrid hash join take a bucket by bucket join and write it to the joinedTable file
			for(int i = 0 ; i < nBuckets ; ++i){
				// form the two file pointers
				File bucketT1 = table1BucketsFileObjects.get(i);
				File bucketT2 = table2BucketsFileObjects.get(i);
				
				// if the size of bucketT2 is less than the size of bucketT1, then we build a HashMap out of bucketT2
				if(bucketT1.length() > bucketT2.length()){
					
					// this HashMap<String, ArrayList<String>> stores the key attribute and the list of strings that contain that key attribute
					HashMap<String, ArrayList<String> > bucketHashJoin = new HashMap<String, ArrayList<String>>();
					// allocate a BufferedReader object for the bucketT2 to form the HashMap
					BufferedReader bucketBR = new BufferedReader(new FileReader(bucketT2));
					String bucketString;
					
					while((bucketString = bucketBR.readLine()) != null){
						// split the bucket string to get the joining attribute's value
						String[] bucketStringSplit = bucketString.split("\\|");
						
						if(bucketHashJoin.containsKey(bucketStringSplit[joiningAttributeIndexTable2])){
							bucketHashJoin.get(bucketStringSplit[joiningAttributeIndexTable2]).add(bucketString);
						}else{
							// form a new array list which will consist of all the strings that contain the key
							ArrayList<String> newList = new ArrayList<String>();
							newList.add(bucketString);
							bucketHashJoin.put(bucketStringSplit[joiningAttributeIndexTable2], newList);
						}
					}
					
					bucketBR.close();
					
					// now probe the HashMap corresponding to bucketT2 with the tuples from bucketT1
					BufferedReader brBucketT1 = new BufferedReader(new FileReader(bucketT1));
					while((bucketString = brBucketT1.readLine()) != null){
						// split the bucket string to get the joining attribute's value
						String[] bucketStringSplit = bucketString.split("\\|");
						
						if(bucketHashJoin.containsKey(bucketStringSplit[joiningAttributeIndexTable1])){
							// this is the list of strings with which to join the bucketString
							ArrayList<String> joinList = bucketHashJoin.get(bucketStringSplit[joiningAttributeIndexTable1]);
							// this is the BufferedWriter object for writing to the joinedTable file
							BufferedWriter tempBW = new BufferedWriter(new FileWriter(joinedTable.tableFilePath, true));
							
							for(String joinString : joinList)
								tempBW.write(bucketString + joinString + "\n");
							
							// close the BufferedWriter
							tempBW.close();
						}
					}
					
				}else{

					
					// this HashMap<String, ArrayList<String>> stores the key attribute and the list of strings that contain that key attribute
					HashMap<String, ArrayList<String> > bucketHashJoin = new HashMap<String, ArrayList<String>>();
					// allocate a BufferedReader object for the bucketT1 to form the HashMap
					BufferedReader bucketBR = new BufferedReader(new FileReader(bucketT1));
					String bucketString;
					
					while((bucketString = bucketBR.readLine()) != null){
						// split the bucket string to get the joining attribute's value
						String[] bucketStringSplit = bucketString.split("\\|");
						
						if(bucketHashJoin.containsKey(bucketStringSplit[joiningAttributeIndexTable1])){
							bucketHashJoin.get(bucketStringSplit[joiningAttributeIndexTable1]).add(bucketString);
						}else{
							// form a new array list which will consist of all the strings that contain the key
							ArrayList<String> newList = new ArrayList<String>();
							newList.add(bucketString);
							bucketHashJoin.put(bucketStringSplit[joiningAttributeIndexTable1], newList);
						}
					}
					
					bucketBR.close();
					
					// now probe the HashMap corresponding to bucketT1 with the tuples from bucketT2
					BufferedReader brBucketT2 = new BufferedReader(new FileReader(bucketT2));
					while((bucketString = brBucketT2.readLine()) != null){
						// split the bucket string to get the joining attribute's value
						String[] bucketStringSplit = bucketString.split("\\|");
						
						if(bucketHashJoin.containsKey(bucketStringSplit[joiningAttributeIndexTable2])){
							// this is the list of strings with which to join the bucketString
							ArrayList<String> joinList = bucketHashJoin.get(bucketStringSplit[joiningAttributeIndexTable2]);
							// this is the BufferedWriter object for writing to the joinedTable file
							BufferedWriter tempBW = new BufferedWriter(new FileWriter(joinedTable.tableFilePath, true));
							
							for(String joinString : joinList)
								tempBW.write(joinString + bucketString + "\n");
							
							// close the BufferedWriter
							tempBW.close();
						}
					}
				}
			}
			
			// delete all the bucket objects that were formed previously, this is done to reduce the clutter
			for(File bucketObject : table1BucketsFileObjects)
				bucketObject.delete();
			for(File bucketObject : table2BucketsFileObjects)
				bucketObject.delete();
		}
		
		// return the joined table formed
		return joinedTable;
	}
}
