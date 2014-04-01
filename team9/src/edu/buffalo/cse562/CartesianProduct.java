package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;

/* This class is used for evaluating the Cartesian product of tables given as input */
public class CartesianProduct {
	
	/* this method is a supplement to the method below and is used to evaluate the Cartesian product of 2 tables */
	public static Table evalCartesianProduct(Table t1 , Table t2) throws IOException{
		
		// calculate the total number of columns in the resulting Cartesian product table
		int totalNoOfColumns = t1.noOfColumns + t2.noOfColumns;
		
		// this is the new table which represents the Cartesian product of the tables given as input
		Table cartesianProduct = new Table(t1.tableName+t2.tableName,t1.dataDirectoryPath,t1.dataDirectoryPath+System.getProperty("file.separator").toString()+t1.tableName+t2.tableName+"Product.dat",totalNoOfColumns);
		
		// populate the column description list of the new Cartesian product table form the column index map of the Cartesian product table
		for(Pair colDescriptionPair : t1.columnDescription){
			
			Pair colDescTypePair;
			
			// make a new pair object to be added to the list of column name and column type pairs in the Cartesian product table
			if(!colDescriptionPair.columnName.contains(".")){
				colDescTypePair = new Pair(t1.tableName+"."+colDescriptionPair.columnName, colDescriptionPair.columnTypeName);
			}else{
				colDescTypePair = new Pair(colDescriptionPair.columnName, colDescriptionPair.columnTypeName);
			}
			
			cartesianProduct.columnDescription.add(colDescTypePair);
		}
		
		for(Pair colDescriptionPair : t2.columnDescription){
			
			Pair colDescTypePair;
			
			// make a new pair object to be added to the list of column name and column type pairs in the Cartesian product table
			if(!colDescriptionPair.columnName.contains(".")){
				colDescTypePair = new Pair(t2.tableName+"."+colDescriptionPair.columnName, colDescriptionPair.columnTypeName);
			}else{
				colDescTypePair = new Pair(colDescriptionPair.columnName, colDescriptionPair.columnTypeName);
			}
			
			cartesianProduct.columnDescription.add(colDescTypePair);
		}
		
		// populate the index map of the Cartesian product table
		cartesianProduct.populateColumnIndexMap();
		
		// now write to the .dat file describing the Cartesian product
		//FileWriter fwr = new FileWriter(new File(cartesianProduct.tableDatFilePath));
		BufferedWriter bwr = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(cartesianProduct.tableDatFilePath))));
		
		// these variables are used to scan the strings from the files describing the 2 tables
		String table1Row;
		String table2Row;
		
		ArrayList<String> tab1 = new ArrayList<String>();
		ArrayList<String> tab2 = new ArrayList<String>();
		while((table1Row = t1.returnTuple()) != null){
			tab1.add(table1Row);
		}
		while((table2Row = t2.returnTuple()) != null){
			tab2.add(table2Row);
		}
		
		//String finalTab[] = new String[tab1.size()*tab2.size()];
		//HashMap<Integer,String> finalTab = new HashMap<Integer, String>();
		//int i = 0;
		for(String s1:tab1){
			for(String s2:tab2){
				//finalTab[i]=s1+"|"+s2;
				//finalTab.put(i, s1+"|"+s2);
				
				bwr.write(s1+"|"+s2);
				bwr.newLine();
				//i++;
			}
		}
		
		tab1 = null;
		tab2 = null;
		//System.gc();
		
		/*for(int s:finalTab.keySet()){
			bwr.write(finalTab.get(s)+"\n");
		}*/
		/*for(String s:finalTab){
			bwr.newLine();
			bwr.write(s);
			
		}*/
		
		
		
		
		/*while((table1Row = t1.returnTuple()) != null){
			while((table2Row = t2.returnTuple()) != null){
				bwr.write(table1Row + "|" + table2Row + "\n");
			}
		}*/
		
		bwr.close();
		
		return cartesianProduct;
	}
	
	/* this method is used to evaluate the Cartesian Product of tables given as input */
	public static Table returnCartesianProduct(ArrayList<Table> listOfTables) throws IOException{
		
		// this is the resultant table we obtain after performing all the Cartesian Product operations
		Table cartesianProductTable;
		
		// if the list of tables is just a null list
		if(listOfTables == null)
			return null;
		
		// if the list of tables just consists of a single table
		else if(listOfTables.size() == 1)
			return listOfTables.get(0);
		
		// if the list of tables consists of just 2 tables
		else if(listOfTables.size() == 2)
			return evalCartesianProduct(listOfTables.get(0), listOfTables.get(1));
		
		// if the list of tables consists of multiple tuples
		else{
			cartesianProductTable = evalCartesianProduct(listOfTables.get(0), listOfTables.get(1));
			
			for(int i = 2 ; i < listOfTables.size() ; ++i){
				cartesianProductTable = evalCartesianProduct(cartesianProductTable, listOfTables.get(i));
			}
		}
		
		return cartesianProductTable;
	}
}
