package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import java.util.List;


public class OrderByOperation {

	public static Table orderBy(Table table, List orderByList,int size) throws IOException {
		
		//System.out.println("in order by");
		
		//System.out.println("in order------"+table);
		//System.out.println(table.columnIndexMap);
		
		//System.out.println(table.columnIndexMap.get("linestatus"));
		
		String  orderByArr[] = new String[size];
		int i=0;
		for(Object o:orderByList){
			orderByArr[i] = o.toString();
			i++;
		}
		
		
		ArrayList<String> array = new ArrayList<String>();
		
			String tuple = null;
			
			int []index = new int[orderByArr.length];
			
			boolean []order = new boolean[orderByArr.length];
			for (int j = 0; j < orderByArr.length; j++) {
				String column = orderByArr[j];
				String colArr[]=column.split(" ");
			
				//System.out.println(table.columnIndexMap.get(colArr[0].trim()));
				
				
					index[j] =  table.columnIndexMap.get(colArr[0]);
					if(colArr.length>1){
						if(colArr[1].equalsIgnoreCase("desc")){

							order[j] = true;
						}
					}
				
			}
			
			
		
			
			
			while((tuple = table.returnTuple()) != null){
				
				String str[] = tuple.split("\\|");
				//String s=str[index];
				array.add(tuple);
 
			}

			Comparator comp = null;
			int cout = 0;
			for (int j = index.length-1; j >= 0; j--) {
				cout++;
			
			
			comp = new AscendingCompare(index[j],order[j]);
			
			
			Collections.sort(array,comp);
			}
		

		
		// this Table contains the resultant table after applying Selection Operation
				/*Table resultantTable = new Table("orderByResultTable",table.dataDirectoryPath,table.dataDirectoryPath+System.getProperty("file.separator").toString()+table.tableName+"OrderBy.dat",table.noOfColumns);
				
				resultantTable.columnDescription = table.columnDescription;
				resultantTable.columnIndexMap = table.columnIndexMap;
				
				FileWriter fwr = new FileWriter(new File(resultantTable.tableDatFilePath));
				BufferedWriter bwr = new BufferedWriter(fwr);
				
				for (int j = 0; j < array.size(); j++) {
				
					bwr.write(array.get(j));
					bwr.write("\n");
					bwr.flush();
				}*/
			
			for(String s:array){
				System.out.println(s);
			}
		
				
		return null;
	}
		
	
}


class AscendingCompare implements Comparator<String>{
	
	int index = 0;
	boolean order = false;

	AscendingCompare(int index,boolean order){
		this.index = index;
		this.order = order;
		
		
	}
	
	
	@Override
	public int compare(String o1, String o2) {
		String s1[] = o1.split("\\|");
		String s2[] = o2.split("\\|");
		
		
		if(s1[index].equals(s2[index])){
				return 0;
		}
		
		else {
			int x =0;
			if(order==false){
				x = s1[index].compareTo(s2[index]); 
				
			}
			else{
				x = s2[index].compareTo(s1[index]);
			}
			
			return x;
		}
		
		
	}
}
