package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/* this class is used to perform the projection operation */
public class ProjectTableOperation {

	/* this method is used to return a table with the project operation applied */
	public static Table projectTable(Table tableToProject,
			ArrayList<String> selectList, boolean flag) throws IOException,
			ParseException {

		// this is the file object for the projected table
		File newTableFile = new File(tableToProject.tableDataDirectoryPath.getAbsolutePath()
						+ System.getProperty("file.separator")
						+ tableToProject.tableName + "ProjectedTable.tbl");
		BufferedWriter bw = new BufferedWriter(new FileWriter(newTableFile),32768);

		Table ProjectedTable = new Table(tableToProject.tableName
				+ "ProjectedTable.tbl", selectList.size(), newTableFile,
				tableToProject.tableDataDirectoryPath);

		// this is the StringBuilder object we use to speed up the writing of the data to the file
		StringBuilder sb = new StringBuilder("");
		
		if (flag == false) {
			for (String str : selectList) {
				String[] temp = str.split(" ");
				if (str.contains(" AS ") || str.contains(" as ")) {
					if (str.contains(" * ") || str.contains(" + ")
							|| str.contains(" - ") || str.contains(" / ")) {

						ColumnDefinition colDescTypePair = new ColumnDefinition();
						ColDataType col = new ColDataType();
						colDescTypePair.setColumnName(temp[temp.length - 1]);
						col.setDataType("Decimal");
						colDescTypePair.setColDataType(col);
						ProjectedTable.columnDescriptionList
								.add(colDescTypePair);
					} else {

						ColumnDefinition colDescTypePair = new ColumnDefinition();
						ColDataType col = new ColDataType();
						colDescTypePair.setColumnName(temp[temp.length - 1]);
						String type = tableToProject.columnDescriptionList
								.get(tableToProject.columnIndexMap.get(temp[0]))
								.getColDataType().getDataType();
						col.setDataType(type);
						colDescTypePair.setColDataType(col);
						ProjectedTable.columnDescriptionList
								.add(colDescTypePair);
					}

				} else {
					ColumnDefinition colDescTypePair = new ColumnDefinition();
					ColDataType col = new ColDataType();
					colDescTypePair.setColumnName(temp[0]);

					String type = "Decimal";
					col.setDataType(type);
					colDescTypePair.setColDataType(col);
					ProjectedTable.columnDescriptionList.add(colDescTypePair);
				}

			}
		}

		if (flag == true) {

			for (String str : selectList) {

				if (str.contains(" AS ") || str.contains(" as ")) {
					String[] temp = str.split(" ");

					ColumnDefinition colDescTypePair = new ColumnDefinition();
					ColDataType col = new ColDataType();
					colDescTypePair.setColumnName(temp[temp.length - 1]);
					String type = tableToProject.columnDescriptionList.get(tableToProject.columnIndexMap.get(temp[temp.length - 1])).getColDataType().getDataType();
					col.setDataType(type);
					colDescTypePair.setColDataType(col);
					ProjectedTable.columnDescriptionList.add(colDescTypePair);
				}

				else if (str.contains("SUM") || str.contains("AVG")
						|| str.contains("sum") || str.contains("avg")
						|| str.contains("min") || str.contains("max")
						|| str.contains("COUNT") || str.contains("count")
						|| str.contains("MIN") || str.contains("MAX")) {
					ColumnDefinition colDescTypePair = new ColumnDefinition();
					ColDataType col = new ColDataType();
					colDescTypePair.setColumnName(str);
					col.setDataType("Int");
					colDescTypePair.setColDataType(col);
					ProjectedTable.columnDescriptionList.add(colDescTypePair);

				} else {
					ColumnDefinition colDescTypePair = new ColumnDefinition();
					ColDataType col = new ColDataType();
					colDescTypePair.setColumnName(str);

					String type = tableToProject.columnDescriptionList
							.get(tableToProject.columnIndexMap.get(str))
							.getColDataType().getDataType();

					col.setDataType(type);
					colDescTypePair.setColDataType(col);
					ProjectedTable.columnDescriptionList.add(colDescTypePair);
				}

			}

		}
		// ProjectedTable.populateColumnIndexMap();

		// this is the reader of the file
		BufferedReader br = new BufferedReader(new FileReader(tableToProject.tableFilePath));
		String canString = br.readLine();
		String[] tupleList;
		String newString = "";
		String columnName = null;

		while (canString != null) {
			newString = "";
			tupleList = canString.split("\\|");
			for (String str : selectList) {
				if (str.contains("*") || str.contains("*") || str.contains("-") || str.contains("+") || str.contains("/")) {

					if (str.contains(" as ") || str.contains(" AS ")) {
						columnName = str.substring(0, str.indexOf(" AS ")).trim();

					} else {
						columnName = str;
					}

					ArrayList<String> arrrStr = WhereOperation.braceExp(columnName);

					String[] pos = new String[arrrStr.size()];
					arrrStr.toArray(pos);
					String[] postFix = WhereOperation.convertToPos(pos);

					String[] arr = new String[postFix.length];

					for (int k = 0; k < postFix.length; k++) {

						if (tableToProject.columnIndexMap.containsKey(postFix[k])) {
							arr[k] = tupleList[tableToProject.columnIndexMap.get(postFix[k])];
						} else {
							arr[k] = postFix[k];
						}
					}
					double val = WhereOperation.evaluate(arr);
					newString = newString + val + "|";
					// System.out.println(newString);

				} else {
					if (flag == true) {
						String columnAdd = tupleList[tableToProject.columnIndexMap.get(str)];
						newString = newString + columnAdd + "|";
					} else {
						if (str.contains("count") && str.contains("distinct")) {
							columnName = str.substring(str.indexOf(" ") + 1,str.length());
						} else {
							columnName = str.substring(0, str.indexOf(" "));
						}
						String columnAdd = tupleList[tableToProject.columnIndexMap.get(columnName)];
						newString = newString + columnAdd + "|";

					}
				}
			}

			canString = br.readLine();
			//bw.write(newString + "\n");
			// append the newString to write to the StringBuilder
			sb.append(newString + "\n");
		}
		// write to the file here
		bw.write(sb.toString());
		bw.close();
		ProjectedTable.populateColumnIndexMap();
		return ProjectedTable;
	}

}