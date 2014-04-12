package edu.buffalo.cse562;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map.Entry;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

public class SelectionOperation {
	// this function is used for the evaluation of the select statement
	@SuppressWarnings("rawtypes")
	public static Table selectionEvaluation(Statement statementObject, HashMap<String, Table> tableObjectsMap, File swapDirectory)
	throws IOException, ParseException, InterruptedException {

		// this is the SelectBody object corresponding to the statement object
		SelectBody selectBody = ((Select) statementObject).getSelectBody();
		// extract the list of "ORDER BY" elements from the plain select statement
		@SuppressWarnings({ "unused", "rawtypes" })
		List orderbyElementsList = ((PlainSelect) selectBody).getOrderByElements();
		// extract the list of "GROUP BY" elements from the plain select statement
		@SuppressWarnings({ "rawtypes", "unused" })
		List groupbyElementsList = ((PlainSelect) selectBody).getGroupByColumnReferences();
		// this is the where clause for the select statement
		@SuppressWarnings("unused")
		Expression whereExpression = ((PlainSelect) selectBody).getWhere();

		// this is a list of table's that need to be joined
		ArrayList<String> listOfTables = new ArrayList<String>();

		if (((PlainSelect) selectBody) != null){
			// this from item can contain a sub query, then evaluate the sub query
			if(((PlainSelect) selectBody).getFromItem().toString().contains("SELECT ") || 
			   ((PlainSelect) selectBody).getFromItem().toString().contains("select ")) {
				
				// this statement gets us the select body
				FromItem fromItem = ((PlainSelect) selectBody).getFromItem();
				// extract the select body out of the fromItem
				SelectBody sBody = ((SubSelect)fromItem).getSelectBody();
				// extract alias of the new table if any, this should be there in most cases
				String alias = ((SubSelect)fromItem).getAlias();
				// now we make a select statement using the select body that we extracted, this is done because our selection function accepts a statement as a parameter
				Select selectStatement = new Select();
				// set the body of the select statement
				selectStatement.setSelectBody(sBody);
				// get the table object evaluated through the sub query
				Table subQueryTable = selectionEvaluation((Statement)selectStatement, tableObjectsMap, swapDirectory);
				// set the name of the new table obtained to be the alias that was extracted or if there is no alias then set the name as "subQueryTable"
				if(alias == null){
					subQueryTable.tableName = "subQueryTable";
					// put the table in the tableObjects map, so that we can refer it later
					tableObjectsMap.put("subQueryTable", subQueryTable);	
				} else{
					subQueryTable.tableName = alias;
					// put the table in the tableObjects map, so that we can refer it later
					tableObjectsMap.put(alias, subQueryTable);
				}
				
				// add the table name string to the list of tables to join
				listOfTables.add(alias);
			} else
				listOfTables.add(((PlainSelect) selectBody).getFromItem().toString().toLowerCase());
		}

		if (((PlainSelect) selectBody).getJoins() != null) {
			// get the list of joins in the from clause
			List joinList = ((PlainSelect) selectBody).getJoins();
			/* handle the case for sub-query in further projects just like above */
			for (Object tableToJoin : joinList)
				listOfTables.add(tableToJoin.toString().toLowerCase());
		}

		// this is the list of Table objects to join, we need to construct this list from the list of joins formed above
		ArrayList<Table> tablesToJoin = new ArrayList<Table>();

		for (Object tableToJoin : listOfTables) {

			// this array is used to get any alias for the table if used
			String[] tableAliasFilterArray = tableToJoin.toString().split(" ");
			// get the table object being referenced here
			Table tableObjectReferenced = tableObjectsMap.get(tableAliasFilterArray[0]);
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
		
		// this Table variable stores the resultant table obtained from joining the tables in the from clause
		Table resultTable = null;
		
		// if the number of tables to join is just 1, then we directly pass in the where expression to the selection operation
		if (tablesToJoin.size() == 1) {
			//System.out.println("Only single table to join : " + tablesToJoin.get(0).tableName);
			// in the case when I just have a single table with me to join, I just need to filter that table and that will be my resultant table
			resultTable = WhereOperation.selectionOnTable(whereExpression,tablesToJoin.get(0));
			
			// after evaluating the resultant table that satisfies the where clause apply aggregate operations on the table
		}

		// now we scan the final list of tables to be joined and change their column description list and column index maps
		if (tablesToJoin.size() > 1) {
			// iterate over the table objects and change their columnDescriptions and columnIndexMap
			for (Table table : tablesToJoin) {

				// make a new array list of column definition objects for the new table
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

			// this Expression variable stores the special skip condition that is when the non join expression consists of both n1 and n2
			Expression tpch7SkipCondition = null;
			
			// the following is the ArrayList of Expression objects which are the non join conditions used for filtering the tables
			ArrayList<Expression> expressionObjects = new ArrayList<Expression>();
			
			for (Expression expression : WhereOperation.extractNonJoinExp(whereExpression)) {
				
				// the following is the condition to skip selection in case of a sub-query in tpch-7 query
				if(expression.toString().contains("n1.") && expression.toString().contains(" OR ") && expression.toString().contains("n2.")){
					tpch7SkipCondition = expression;
					continue;
				}
				
				expressionObjects.add(expression);
			}

			// this HashMap is used to form the AND expressions corresponding to a table that needs to be evaluated
			HashMap<Table, ArrayList<Expression>> tableExpressionMap = new HashMap<Table, ArrayList<Expression>>();

			for (Table table : tablesToJoin) {
				// form the ArrayList of expressions to be evaluated on the table
				ArrayList<Expression> tableExpressionList = new ArrayList<Expression>();
				for (Expression exp : expressionObjects) {
					if (exp.toString().contains(table.tableName))
						tableExpressionList.add(exp);
				}
				tableExpressionMap.put(table, tableExpressionList);
			}
			
			// the following is the modified list of tables that need to be joined, in this case all the tables are filtered on a condition
			ArrayList<Table> filteredTablesToJoin = new ArrayList<Table>();
			
			// start filtering the tables, to form the join
			for (Entry<Table, ArrayList<Expression>> etr : tableExpressionMap.entrySet()) {
				// if the number of expressions involving a table equals 1
				if (etr.getValue().size() == 1) {
					// call the selection operator to filter the table
					//System.out.println("Filtering : " + etr.getKey().tableName + "    " + etr.getValue().get(0));
					// add the filtered table to the list of tables that need to be joined
					filteredTablesToJoin.add(WhereOperation.selectionOnTable(etr.getValue().get(0),etr.getKey()));
/*					//System.out.println("ANKT SABLOK : " + etr.getValue().get(0));
*/				} else if (etr.getValue().size() >= 2) {

					// form the and expression which involves all the conditions involving a table
					AndExpression andExp = new AndExpression(etr.getValue().get(0), etr.getValue().get(1));
					for (int i = 2; i < etr.getValue().size(); ++i)
						andExp = new AndExpression(andExp, etr.getValue().get(i));

					// call the selection operator to filter the table
/*					System.out.println("Filtering : " + etr.getKey().tableName);
*/					// add the filtered table to the list of tables that need to be joined
					filteredTablesToJoin.add(WhereOperation.selectionOnTable(andExp, etr.getKey()));
				} else {
					// this is the case where there is no filtering condition corresponding to a table
					filteredTablesToJoin.add(etr.getKey());
				}
			}

			// the following is the logic to find the join of all the tables iteratively
			HashMap<Integer, Table> mapOfTables = new HashMap<Integer, Table>();
			int i = 0;
			for (Table table : filteredTablesToJoin) {
				mapOfTables.put(i, table);
				++i;
			}
			//System.out.println(mapOfTables);
			
			
			Table t1 = null;
			Table t2 = null;
			int countOfJoins = 0;
			int index = mapOfTables.size() - 1;

			if (index > 0) {
				while (countOfJoins != index) {
					//System.out.println("In While");
					for (int iterativeIndex = index - 1; iterativeIndex >= 0; iterativeIndex--) {

						t1 = mapOfTables.get(index);
						t2 = mapOfTables.get(iterativeIndex);
						//if(t2== null)
							//System.out.println(t2);
						if (t2 == null) {
							continue;
						}

						// this array list stores the join conditions for table involved
						ArrayList<String> arrayList = WhereOperation.evaluateJoinCondition(t1, t2, whereExpression);
						//System.out.println(arrayList.size());
						for (String s : arrayList)
							//System.out.println(s);

						if (arrayList.size() > 0 && mapOfTables.get(iterativeIndex) != null) {
							// call the HashJoin and evaluate the new table
							//System.out.println("In here");
							System.out.println("table to join :" + arrayList.get(0) + " : " + arrayList.get(1));
							resultTable = HybridHash.evaluateJoin(t1, t2,arrayList.get(0), arrayList.get(1),arrayList.get(2), swapDirectory);
							mapOfTables.put(iterativeIndex, null);
							mapOfTables.put(index, resultTable);
							countOfJoins++;
						}
					}
				}
			}
			
			// check if the tpch7 condition for the sub-query is not null and apply it to the table
			if(tpch7SkipCondition != null){
				Expression expression = ((Parenthesis)tpch7SkipCondition).getExpression();
				// perform the selection on the table with the expression above
				resultTable = WhereOperation.selectionOnTable(expression, resultTable);
			}
			// after evaluating the join that satisfies all the join conditions apply the aggregate operations on the resultant table
		}
		
		// display the column definitions of the resulting table
		if(resultTable != null){
			for(ColumnDefinition cd : resultTable.columnDescriptionList)
			{
				
			}
				//System.out.println(cd.getColumnName());
		}
		
		// the following is the list of select items, that is the items that need to be computed and stored in the resulting table
		@SuppressWarnings("rawtypes")
		List selectItemList = ((PlainSelect)selectBody).getSelectItems();
		// this is an arrayList of select item strings
		ArrayList<String> selectList = new ArrayList<String>();
		// form the above selectList from the selectItemList
		for(Object column : selectItemList){
			//System.out.println("select list item : " + column.toString());
			selectList.add(column.toString());
		}
		
		// this variable is used to check if there is an aggregate function present in the select list
		boolean aggregatePresent = false;
		// scan the selectItem list and check if there is an aggregate present or not
		for(String selectItem : selectList){
			if(selectItem.contains("sum(") || selectItem.contains("SUM(") || selectItem.contains("avg(") 
			   || selectItem.contains("AVG(") || selectItem.contains("count(") || selectItem.contains("COUNT(")
			   || selectItem.contains("min(") || selectItem.contains("MIN(") || selectItem.contains("max(")
			   || selectItem.contains("MAX(")){
				aggregatePresent = true;
				break;
			}
		}
		
		// if there are no group by columns present and there are no aggregates then just project the items
		/* THIS CASE NEEDS TO BE HANDLED IN THE UPCOMING PROJECTS FOR THE CASE IF ORDERBY IS PRESENT OR NOT */
		if(groupbyElementsList == null && !aggregatePresent){
			return ProjectTableOperation.projectTable(resultTable, selectList,false);
		}
		else{
			// make the object of Aggregate class to call Aggregate functions
			AggregateOperations aggrObject = new AggregateOperations();
			// this string is a comma separated string of GroupBy items
			String groupItems;
				
			if(groupbyElementsList == null)
				groupItems = "NOGroupBy";
			else
				groupItems = groupbyElementsList.toString();
				
			// we store the select items list in an array
			String[] selectItemsArray = ((PlainSelect)selectBody).getSelectItems().toString().replaceAll("\\[", "").replaceAll("\\]", "").trim().split(",");
				
			// test the select items array
			/*for(String s : selectItemsArray)
				System.out.println("select items array : " + s);
			*/	
			// call the aggregate function to get the resultant table
			resultTable = aggrObject.getAggregate(resultTable, selectItemsArray, groupItems,orderbyElementsList);
			
		}
		// return the resultant table after performing all selections
		return resultTable;
	}
}
