package edu.buffalo.cse562;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class SelectionOperation {
	public static void selectionEvaluation(Statement statementObject,
			HashMap<String, Table> tableObjectsMap, File swapDirectory) {

		// this is the SelectBody object corresponding to the statement object
		SelectBody selectBody = ((Select) statementObject).getSelectBody();
		// extract the list of "ORDER BY" elements from the plain select statement
		List orderbyElementsList = ((PlainSelect) selectBody)
				.getOrderByElements();
		// extract the list of "GROUP BY" elements from the plain select statement
		List groupbyElementsList = ((PlainSelect) selectBody)
				.getGroupByColumnReferences();
		// extract the "LIMIT" value in the plain select statement
		Limit limit = ((PlainSelect) selectBody).getLimit();
		// this is the where clause for the select statement
		Expression whereExpression = ((PlainSelect) selectBody).getWhere();
		// this is a list of table's that need to be joined
		ArrayList<String> listOfTables = new ArrayList<String>();

		if (((PlainSelect) selectBody) != null)
			listOfTables.add(((PlainSelect) selectBody).getFromItem()
					.toString());
		if (((PlainSelect) selectBody).getJoins() != null) {
			List joinList = ((PlainSelect) selectBody).getJoins();
			for (Object tableToJoin : joinList)
				listOfTables.add(tableToJoin.toString());
		}
		
		// this is the list of Table objects to join
		ArrayList<Table> tablesToJoin = new ArrayList<Table>();
		
		for(Object tableToJoin : listOfTables){
			String[] tableAliasFilterArray = tableToJoin.toString().split(" ");
			if(tableAliasFilterArray.length > 1){
				// get the table object being referred here
				Table tableObjectReferenced = tableObjectsMap.get(tableAliasFilterArray[0]);
				tableObjectsMap.put(tableAliasFilterArray[2], tableObjectReferenced);
				tablesToJoin.add(tableObjectReferenced);
			}else{
				tablesToJoin.add(tableObjectsMap.get(tableAliasFilterArray[0]));
			}
		}
	}
}
