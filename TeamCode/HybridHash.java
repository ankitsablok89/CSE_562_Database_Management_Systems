package edu.buffalo.cse562;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class HybridHash {

	// this function is used to evaluate the hash join of the tables based on
	// the joining attribute, the return type of the function is the joined table's name
	public static String evaluateJoin(Table t1, Table t2, String joiningAttribute, File swapDirectory) throws IOException {
		
		// get the indexes of the joining attribute in table1 and table2 to index the tuples in the correct manner
		int joiningAttributeIndexTable1 = t1.columnIndexMap.get(joiningAttribute);
		int joiningAttributeIndexTable2 = t2.columnIndexMap.get(joiningAttribute); 
		
		// if the swap directory is a null value in that case everything needs to be handled in memory
		if(swapDirectory == null){
			// this is the file pointer for table1
			File tableFile1 = t1.tableFilePath;
			// this is the file pointer for table2
			File tableFile2 = t2.tableFilePath;
			// this HashMap< String, ArrayList<String> > is interpreted as the following HashMap<joinAttribute, List of Strings that contain that attribute>
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
						hashJoinTable.put(tupleComponents[joiningAttributeIndexTable2], newTupleStringsList);
					}
				}
			} else{
				
				// scan the strings in the tableFile1 and put them into HashMap
				String tupleString;
				while((tupleString = t1.returnTuple()) != null){
					// this string array stores different components of a tuple
					String[] tupleComponents = tupleString.split("\\|");
					if(hashJoinTable.containsKey(tupleComponents[joiningAttributeIndexTable1]))
						hashJoinTable.get(tupleComponents[joiningAttributeIndexTable1]).add(tupleString);
					else{
						// make a new ArrayList of String objects which would hold the tuples that have same value for the key attribute
						ArrayList<String> newTupleStringsList = new ArrayList<String>();
						hashJoinTable.put(tupleComponents[joiningAttributeIndexTable1], newTupleStringsList);
					}
				}
			}
		}

	}
}
