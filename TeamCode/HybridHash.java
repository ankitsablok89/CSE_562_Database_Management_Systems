package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class HybridHash {

	// this function is used to evaluate the hash join of the tables based on
	// the joining attribute, the return type of the function is the joined table's name
	public static String evaluateJoin(Table t1, Table t2, String joiningAttribute, File swapDirectory) {
		
		// if the swap directory is a null value in that case everything needs to be handled in memory
		if(swapDirectory == null){
			// this is the file pointer for table1
			File table1 = t1.tableFilePath;
			// this is the file pointer for table2
			File table2 = t2.tableFilePath;
			// this HashMap< String, ArrayList<String> > is interpreted as the following HashMap<joinAttribute, List of Strings that contain that attribute>
			HashMap<String, ArrayList<String> > hashJoinTable = new HashMap<String, ArrayList<String>>();
			
			// if the size of the first table is greater than the size of the other table then we store the other table in the HashMap
			if(table1.length() > table2.length()){
				
			}
		}

	}
}
