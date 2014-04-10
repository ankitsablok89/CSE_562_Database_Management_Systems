package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/* this class is used to perform the projection operation */
public class ProjectTableOperation {
	
	/* this method is used to return a table with the project operation applied */
	public static Table projectTable(Table tableToProject,ArrayList<String> projectColumnList) throws IOException{
		
		// this is the new table object that holds the final projected result
		File ProjectedtableFile=new File(tableToProject.tableDataDirectoryPath+System.getProperty("file seperator")+"Projected" + tableToProject.tableName+".tbl");
		Table projectTable = new Table("Projected" + tableToProject.tableName, projectColumnList.size(),ProjectedtableFile,tableToProject.tableDataDirectoryPath );
		
		// populate the column description list of the table
		for(int i = 0 ; i < projectColumnList.size() ; ++i){
			
			String columnName = projectColumnList.get(i);
			ColDataType col=new ColDataType();
			for(ColumnDefinition colDef :projectTable.columnDescriptionList){
				
				if(colDef.getColumnName().equals(columnName)){
					
					colDef.setColumnName(projectColumnList.get(i));
					col.setDataType("String");
					colDef.setColDataType(col);
	                projectTable.columnDescriptionList.add(colDef);				}
			}
		}
		
		// populate the column index map of the new table
		projectTable.populateColumnIndexMap();
		
		// now write the projected tuples to the file corresponding to the new table
		FileWriter fw = new FileWriter(projectTable.tableFilePath);
		BufferedWriter bwr = new BufferedWriter(fw);
		
		// this string is used to read the table that is to be projected
		String scanString;
		while( (scanString = tableToProject.returnTuple()) != null ){
			// this string array is used to store the tokens in the string
			String[] tokens = scanString.split("\\|");
			
			for(int i = 0 ; i < projectTable.columnDescriptionList.size() ; ++i){
				
				if(i == projectTable.columnDescriptionList.size() - 1){
					bwr.write(tokens[tableToProject.columnIndexMap.get(projectTable.columnDescriptionList.get(i).getColumnName())]+"\n");
				}else{
					bwr.write(tokens[tableToProject.columnIndexMap.get(projectTable.columnDescriptionList.get(i).getColumnName())]+"|");
				}
			}
		}
		
		// close the buffered reader object
		bwr.close();
		
		return projectTable;
	}
}