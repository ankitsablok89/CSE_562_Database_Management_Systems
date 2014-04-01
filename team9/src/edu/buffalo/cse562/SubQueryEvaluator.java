package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class SubQueryEvaluator {
	
	/* this function is used to return the table that has been obtained by evaluating the sub-query */
	public static Table evaluateSubQuery(String subQuery) throws ParseException, IOException {
		
		// this parser object is used to parse the sub-query
		CCJSqlParser parserObjSqlParser = new CCJSqlParser(new StringReader(subQuery));
		// this variable is used to store the select body
		SelectBody selectBody = ((Select)parserObjSqlParser.Statement()).getSelectBody();
		// this is the PlainSelect statement corresponding to the SQL sub-query
		PlainSelect selectStmt = ((PlainSelect)selectBody);
		// this is the new table object that will be returned to the calling method
		Table newTable = new Table(selectStmt.getFromItem().toString() + "subQueryTable", Main.tableMap.get(selectStmt.getFromItem().toString().toLowerCase()).dataDirectoryPath,Main.tableMap.get(selectStmt.getFromItem().toString().toLowerCase()).dataDirectoryPath+System.getProperty("file.separator")+selectStmt.getFromItem().toString() + "subQueryTable.dat",selectStmt.getSelectItems().size());
		// this is the list of select items in the nested query
		@SuppressWarnings({ "unused", "rawtypes" })
		List selectItems = selectStmt.getSelectItems();
		// this is the array list of select items in the string
		ArrayList<String> selectList = new ArrayList<String>();
		for(Object col : selectItems)
			selectList.add(col.toString());
		
		// this is the table that we need to read in the from item
		Table tableToRead = Main.tableMap.get(selectStmt.getFromItem().toString().toLowerCase());
		
		// allocate writer objects that point to the file containing the result of the sub-query
		FileWriter fwr = new FileWriter(new File(newTable.tableDatFilePath));
		BufferedWriter bwr = new BufferedWriter(fwr);
		
		// this string is used to read the tuple in the file
		String scanString;
		while((scanString = tableToRead.returnTuple()) != null){
			// store the tuple in the form of array of tokens
			String[] tokens = scanString.split("\\|");
			
			// this is the tuple that we will store in the file
			String newTuple = "";
			
			// iterate over the list of columns or attributes that need to be projected
			for(int i = 0 ; i < selectList.size() ; ++i){
				
				// if the sub-query expression contains AS
				if(selectList.get(i).contains(" AS ")){
					// split the select item based on spaces
					String[] splitItem = selectList.get(i).split(" ");
					
					// this variable stores the index of "AS"
					int asIndex = 0;
					
					for(asIndex = 0 ; asIndex < splitItem.length ; ++asIndex){
						if(splitItem[asIndex].equals("AS"))
							break;
					}
					
					// this is the actual index of the AS in the sub-query
					--asIndex;
					
					if(i == selectList.size() - 1)
						newTuple += Integer.toString((int)SelectionOperation.evaluate(SelectionOperation.convertToPos(new String[]{tokens[tableToRead.columnIndexMap.get(splitItem[0])], splitItem[1], tokens[tableToRead.columnIndexMap.get(splitItem[2])]})));
					else
						newTuple += Integer.toString((int)SelectionOperation.evaluate(SelectionOperation.convertToPos(new String[]{tokens[tableToRead.columnIndexMap.get(splitItem[0])], splitItem[1], tokens[tableToRead.columnIndexMap.get(splitItem[2])]}))) + "|";
				}else{
					
					if(i == selectList.size() - 1){
						newTuple += tokens[tableToRead.columnIndexMap.get(selectList.get(i))];
					}else{
						newTuple += tokens[tableToRead.columnIndexMap.get(selectList.get(i))] + "|";
					}
				}
			}
			
			bwr.write(newTuple + "\n");
		}
		
		bwr.close();
		
		// this is the array list of the newly created subquery table
		ArrayList<Pair> columnDescription = new ArrayList<Pair>(); 
		
		for(int i = 0 ; i < selectList.size() ; ++i){
			if(selectList.get(i).contains(" AS ")){
				newTable.columnDescription.add(new Pair(selectList.get(i).split(" ")[selectList.get(i).split(" ").length - 1] , ""));
			}else
				newTable.columnDescription.add(new Pair(selectList.get(i), ""));
		}
		
		newTable.populateColumnIndexMap();
		
		return newTable;
	}
}
