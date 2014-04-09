package edu.buffalo.cse562;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map.Entry;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

public class SelectionOperation {
	// this function is used for the evaluation of the select statement
	@SuppressWarnings("rawtypes")
	public static void selectionEvaluation(Statement statementObject, HashMap<String, Table> tableObjectsMap, File swapDirectory)
	throws IOException, ParseException {

		// this is the SelectBody object corresponding to the statement object
		SelectBody selectBody = ((Select) statementObject).getSelectBody();
		// extract the list of "ORDER BY" elements from the plain select statement
		@SuppressWarnings({ "unused", "rawtypes" })
		List orderbyElementsList = ((PlainSelect) selectBody)
				.getOrderByElements();
		// extract the list of "GROUP BY" elements from the plain select statement
		@SuppressWarnings({ "rawtypes", "unused" })
		List groupbyElementsList = ((PlainSelect) selectBody)
				.getGroupByColumnReferences();
		// extract the "LIMIT" value in the plain select statement
		@SuppressWarnings("unused")
		Limit limit = ((PlainSelect) selectBody).getLimit();
		// this is the where clause for the select statement
		@SuppressWarnings("unused")
		Expression whereExpression = ((PlainSelect) selectBody).getWhere();

		// this is a list of table's that need to be joined
		ArrayList<String> listOfTables = new ArrayList<String>();

		if (((PlainSelect) selectBody) != null)
			listOfTables.add(((PlainSelect) selectBody).getFromItem()
					.toString().toLowerCase());

		if (((PlainSelect) selectBody).getJoins() != null) {
			List joinList = ((PlainSelect) selectBody).getJoins();
			for (Object tableToJoin : joinList)
				listOfTables.add(tableToJoin.toString().toLowerCase());
		}

		// this is the list of Table objects to join, we need to construct this list from the list of joins formed above
		ArrayList<Table> tablesToJoin = new ArrayList<Table>();

		for (Object tableToJoin : listOfTables) {

			// this array is used to get any alias for the table if used
			String[] tableAliasFilterArray = tableToJoin.toString().split(" ");
			// get the table object being referenced here
			Table tableObjectReferenced = tableObjectsMap
					.get(tableAliasFilterArray[0]);
			// if the table in the from clause has an alias
			if (tableAliasFilterArray.length > 1) {
				// construct a new table object
				Table newTable = new Table(tableObjectReferenced);
				// change the name of the table
				newTable.tableName = tableAliasFilterArray[2].toLowerCase();
				// put the Table object in the map
				tableObjectsMap.put(newTable.tableName, newTable);
				// add the new table to tablesToJoin list
				tablesToJoin.add(newTable);
			} else
				tablesToJoin.add(tableObjectsMap.get(tableAliasFilterArray[0]));
		}

		// if the number of tables to join is just 1, then we directly pass in the where expression to the selection operation
		if (tablesToJoin.size() == 1) {
			// just filter the table
			WhereOperation.selectionOnTable(whereExpression,tablesToJoin.get(0));
		}

		// now we scan the final list of tables to be joined and change their column description list and column index maps
		if (tablesToJoin.size() > 1) {
			// iterate over the table objects and change their columnDescriptions and columnIndexMap
			for (Table table : tablesToJoin) {

				// make a new array list of column definition objects for the
				// new table
				ArrayList<ColumnDefinition> colDefinitionList = new ArrayList<ColumnDefinition>();
				for (ColumnDefinition cd : table.columnDescriptionList) {
					ColumnDefinition temp = new ColumnDefinition();
					temp.setColumnName(table.tableName + "." + cd.getColumnName());
					temp.setColDataType(cd.getColDataType());
					colDefinitionList.add(temp);
				}

				// set the new columnDefinitionList
				table.columnDescriptionList = colDefinitionList;

				// make a new column index map
				HashMap<String, Integer> colIndexMap = new HashMap<String, Integer>();
				for (Entry<String, Integer> etr : table.columnIndexMap.entrySet()) {
					colIndexMap.put(table.tableName + "." + etr.getKey(),etr.getValue());
				}

				// set the new column index map
				table.columnIndexMap = colIndexMap;
			}

			// the following is the ArrayList of Expression objects
			ArrayList<Expression> expressionObjects = new ArrayList<Expression>();

			for (Expression expression : WhereOperation.extractNonJoinExp(whereExpression)) {
				expressionObjects.add(expression);
			}

			// this HashMap is used to form the AND expressions corresponding to a table that needs to be evaluated
			HashMap<Table, ArrayList<Expression>> tableExpressionMap = new HashMap<Table, ArrayList<Expression>>();

			for (Table table : tablesToJoin) {
				ArrayList<Expression> tableExpressionList = new ArrayList<Expression>();
				for (Expression exp : expressionObjects) {
					if (exp.toString().contains(table.tableName))
						tableExpressionList.add(exp);
				}
				tableExpressionMap.put(table, tableExpressionList);
			}

			// start filtering the tables
			for (Entry<Table, ArrayList<Expression>> etr : tableExpressionMap.entrySet()) {
				// if the number of expressions involving a table equals 1
				if (etr.getValue().size() == 1) {
					// call the selection operator to filter the table
					System.out.println("Filtering : " + etr.getKey().tableName);
					WhereOperation.selectionOnTable(etr.getValue().get(0),etr.getKey());
				} else if (etr.getValue().size() >= 2) {

					// form the and expression which involves all the conditions
					// involving a table
					AndExpression andExp = new AndExpression(etr.getValue().get(0), etr.getValue().get(1));
					for (int i = 2; i < etr.getValue().size(); ++i)
						andExp = new AndExpression(andExp, etr.getValue().get(i));

					// call the selection operator to filter the table
					System.out.println("Filtering : " + etr.getKey().tableName);
					WhereOperation.selectionOnTable(andExp, etr.getKey());
				} else {

				}
			}

			// the following is the logic to find the join of all the tables iteratively
			HashMap<Integer, Table> mapOfTables = new HashMap<Integer, Table>();
			int i = 0;
			for (Table table : tablesToJoin) {
				mapOfTables.put(i, table);
				++i;
			}

			Table t1 = null;
			Table t2 = null;
			int countOfJoins = 0;
			int index = mapOfTables.size() - 1;

			if (index > 0) {
				while (countOfJoins != index) {

					for (int iterativeIndex = index - 1; iterativeIndex >= 0; iterativeIndex--) {

						t1 = mapOfTables.get(index);
						t2 = mapOfTables.get(iterativeIndex);
						if (t2 == null) {
							continue;
						}

						// this array list stores the join conditions for table involved
						ArrayList<String> arrayList = WhereOperation.evaluateJoinCondition(t1, t2, whereExpression);

						/*for (String s : arrayList)
							System.out.println(s);*/

						if (arrayList.size() > 0 && mapOfTables.get(iterativeIndex) != null) {
							// call the HashJoin and evaluate the new table
							Table newTable = HybridHash.evaluateJoin(t1, t2,arrayList.get(0), arrayList.get(1),arrayList.get(2), swapDirectory);
							mapOfTables.put(iterativeIndex, null);
							mapOfTables.put(index, newTable);
							countOfJoins++;
						}
					}
				}
			}
		}

	}
}
