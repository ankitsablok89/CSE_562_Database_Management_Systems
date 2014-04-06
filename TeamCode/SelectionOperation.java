package edu.buffalo.cse562;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map.Entry;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class SelectionOperation {
	// this function is used for the evaluation of the select statement
	@SuppressWarnings("rawtypes")
	public static void selectionEvaluation(Statement statementObject,HashMap<String, Table> tableObjectsMap, File swapDirectory) throws IOException {

		// this is the SelectBody object corresponding to the statement object
		SelectBody selectBody = ((Select) statementObject).getSelectBody();
		// extract the list of "ORDER BY" elements from the plain select statement
		@SuppressWarnings({ "unused", "rawtypes" })
		List orderbyElementsList = ((PlainSelect) selectBody).getOrderByElements();
		// extract the list of "GROUP BY" elements from the plain select statement
		@SuppressWarnings({ "rawtypes", "unused" })
		List groupbyElementsList = ((PlainSelect) selectBody).getGroupByColumnReferences();
		// extract the "LIMIT" value in the plain select statement
		@SuppressWarnings("unused")
		Limit limit = ((PlainSelect) selectBody).getLimit();
		// this is the where clause for the select statement
		@SuppressWarnings("unused")
		Expression whereExpression = ((PlainSelect) selectBody).getWhere();

		// this is a list of table's that need to be joined
		ArrayList<String> listOfTables = new ArrayList<String>();

		if (((PlainSelect) selectBody) != null)
			listOfTables.add(((PlainSelect) selectBody).getFromItem().toString().toLowerCase());
		
		if (((PlainSelect) selectBody).getJoins() != null) {
			List joinList = ((PlainSelect) selectBody).getJoins();
			for (Object tableToJoin : joinList)
				listOfTables.add(tableToJoin.toString().toLowerCase());
		}
		
		// this is the list of Table objects to join, we need to construct this list from the list of joins formed above
		ArrayList<Table> tablesToJoin = new ArrayList<Table>();
		
		for(Object tableToJoin : listOfTables){
			
			// this array is used to get any alias for the table if used
			String[] tableAliasFilterArray = tableToJoin.toString().split(" ");
			// get the table object being referenced here
			Table tableObjectReferenced = tableObjectsMap.get(tableAliasFilterArray[0]);
			// if the table in the from clause has an alias
			if(tableAliasFilterArray.length > 1){
				// construct a new table object
				Table newTable = new Table(tableObjectReferenced);
				// change the name of the table
				newTable.tableName = tableAliasFilterArray[2].toLowerCase();
				// put the Table object in the map
				tableObjectsMap.put(newTable.tableName, newTable);
				tablesToJoin.add(newTable);
			}
			else
				tablesToJoin.add(tableObjectsMap.get(tableAliasFilterArray[0]));
		}
		
		// now we scan the final list of tables to be joined and change their column description list and column index maps
		if(tablesToJoin.size() > 1){
			// iterate over the table objects and change their columnDescriptions and columnIndexMap
			for(Table table : tablesToJoin){
				
				// make a new array list of column definition objects for the new table
				ArrayList<ColumnDefinition> colDefinitionList = new ArrayList<ColumnDefinition>();
				for(ColumnDefinition cd : table.columnDescriptionList){
					ColumnDefinition temp = new ColumnDefinition();
					temp.setColumnName(table.tableName + "." + cd.getColumnName());
					temp.setColDataType(cd.getColDataType());
					colDefinitionList.add(temp);
				}
				
				// set the new columnDefinitionList
				table.columnDescriptionList = colDefinitionList;
				
				// make a new column index map
				HashMap<String, Integer> colIndexMap = new HashMap<String, Integer>();
				for(Entry<String, Integer> etr : table.columnIndexMap.entrySet()){
					colIndexMap.put(table.tableName + "." + etr.getKey(), etr.getValue());
				}
				
				// set the new column index map
				table.columnIndexMap = colIndexMap;
			}	
		}
		
		/*LOGIC TO EXTRACT THE JOIN CONDITIONS AND JOIN TABLES*/
		HybridHash.evaluateJoin(tablesToJoin.get(0), tablesToJoin.get(1),"custkey", swapDirectory);
	}
}
