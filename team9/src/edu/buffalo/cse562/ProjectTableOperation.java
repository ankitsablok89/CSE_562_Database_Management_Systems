package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/* this class is used to perform the projection operation */
public class ProjectTableOperation {
	
	/* this method is used to return a table with the project operation applied */
	public static Table projectTable(Table tableToProject,ArrayList<String> projectColumnList) throws IOException{
		
		// this is the new table object that holds the final projected result
		Table projectTable = new Table("Projected" + tableToProject.tableName,tableToProject.dataDirectoryPath,tableToProject.dataDirectoryPath + System.getProperty("file.separator") + "Projected" + tableToProject.tableName + ".dat", projectColumnList.size());
		
		// populate the column description list of the table
		for(int i = 0 ; i < projectColumnList.size() ; ++i){
			
			String columnName = projectColumnList.get(i);
			
			for(Pair columnNameTypePair : tableToProject.columnDescription){
				
				if(columnNameTypePair.columnName.equals(columnName)){
					Pair projectColumnNameType = new Pair(columnNameTypePair);
					projectTable.columnDescription.add(projectColumnNameType);
				}
			}
		}
		
		// populate the column index map of the new table
		projectTable.populateColumnIndexMap();
		
		// now write the projected tuples to the file corresponding to the new table
		FileWriter fw = new FileWriter(new File(projectTable.tableDatFilePath));
		BufferedWriter bwr = new BufferedWriter(fw,32768);
		
		// this string is used to read the table that is to be projected
		String scanString;
		while( (scanString = tableToProject.returnTuple()) != null ){
			// this string array is used to store the tokens in the string
			String[] tokens = scanString.split("\\|");
			
			for(int i = 0 ; i < projectTable.columnDescription.size() ; ++i){
				
				if(i == projectTable.columnDescription.size() - 1){
					bwr.write(tokens[tableToProject.columnIndexMap.get(projectTable.columnDescription.get(i).columnName)]+"\n");
				}else{
					bwr.write(tokens[tableToProject.columnIndexMap.get(projectTable.columnDescription.get(i).columnName)]+"|");
				}
			}
		}
		
		// close the buffered reader object
		bwr.close();
		
		return projectTable;
	}
}


