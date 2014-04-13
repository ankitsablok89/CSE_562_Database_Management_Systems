package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;


public class ExternalSort {
	
	private static int phase = 0;
	// method to perform the External merge Sort
	public static Table performExternalMergeSort(Table table, List orderByList) throws IOException {
		//System.out.println("in ");
		//System.out.println(table.tableFilePath);

		//System.out.println(table.tableDataDirectoryPath);
		Collections.reverse(orderByList);
		ArrayList<Table> chunkList = sorting(table,orderByList,orderByList.size());
		return merging(chunkList, orderByList, orderByList.size());
		//System.out.println(chunkList.size());
		//return table;
	}
	
	// this method implements the sorting phase of the external merge sort
		public static ArrayList<Table> sorting(Table table, List orderByList,int size){
			//System.out.println(Runtime.getRuntime().freeMemory()+ " "+ Runtime.getRuntime().totalMemory() + " "+Runtime.getRuntime().maxMemory());
			//System.out.println(Runtime.getRuntime().maxMemory()-(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()));
			
			
			//System.out.println("in sorting");
			
			// fixed chunk size
			int chunkSize = 10000;
			
			// number of chunks
			int chunkCount = 0;
			
			// map containing the list of chunks ie the chunk number and respective Table object 
			HashMap<Integer,Table> chunkList = new HashMap<Integer,Table>();
			
			// newTable is the chunk table added in the chunkList			
			Table newTable = null;
						
			FileWriter fwr = null;
			BufferedWriter bwr = null;

			// One tuple
			String tuple = null;
			
			// this arrayList stores the tuples and it is used to sort those tuples in each chunk
			ArrayList<String> rows = new ArrayList<String>();
			
			// this count points to the number of tuples added to one particular chunk
			int count=0;
			try {
				
				/*newTable = new Table("Temp" + chunkCount+table.tableName,table.dataDirectoryPath,table.dataDirectoryPath + System.getProperty("file.separator").toString() + "Temp" +chunkCount +table.tableName + ".dat", table.noOfColumns);
				newTable.columnDescription = table.columnDescription;
				newTable.columnIndexMap = table.columnIndexMap;
				fwr = new FileWriter(new File(newTable.tableDatFilePath));
				bwr = new BufferedWriter(fwr);*/
				MergingComparator sortingComp = new MergingComparator(table, orderByList, size);
				
				while((tuple = table.returnTuple()) != null){
					//System.out.println(tuple);
					
					//System.out.println("in");
					count++;
					if(count<= chunkSize){
						//System.out.println("data added");
						rows.add(tuple);
					}
					// in else because the chunk is full and the tuples are to be written on that particular chunk 
					else{
				//		System.out.println("else");
						
												
						Collections.sort(rows,sortingComp);
						
						
						newTable = new Table("temp_outside" + chunkCount+table.tableName,table.noOfColumns,new File(table.tableDataDirectoryPath+"\\"+"temp_outside" + chunkCount+table.tableName),table.tableDataDirectoryPath);
						newTable.columnDescriptionList = table.columnDescriptionList;
						newTable.columnIndexMap = table.columnIndexMap;
						fwr = new FileWriter(newTable.tableFilePath);
						bwr = new BufferedWriter(fwr);
						
						
						for(String row:rows){
							bwr.write(row+"\n");
							//bwr.write("\n");
							//bwr.flush();
							}
						bwr.close();
						//sorting the table according to order By clause
						//newTable= OrderByOperation.orderBy(newTable, orderByList, orderByList.size());

											
						// putting the identity of chunk in chunkList
						chunkList.put(chunkCount,newTable);
						
						chunkCount++;
						newTable = null;
						// resetting the count so that data is written in new chunk
						count=1;
						rows = new ArrayList<String>();
						rows.add(tuple);
					}
					
				}
				/*System.out.println("-------");
				System.out.println(table.columnDescriptionList);
				System.out.println(table.columnIndexMap);*/
				//System.out.println("outside");
				Collections.sort(rows,sortingComp);
				
				newTable = new Table("temp_outside" + chunkCount+table.tableName,table.noOfColumns,new File(table.tableDataDirectoryPath+"\\"+"temp_outside" + chunkCount+table.tableName),table.tableDataDirectoryPath);
				newTable.columnDescriptionList = table.columnDescriptionList;
				newTable.columnIndexMap = table.columnIndexMap;
				//System.out.println(newTable.tableFilePath);
				fwr = new FileWriter(newTable.tableFilePath);
				bwr = new BufferedWriter(fwr);
				
				
				if(rows.size()>0){
					//System.out.println("size greater");
					for(String row:rows){
						bwr.write(row+"\n");
						//bwr.write("\n");
						//bwr.flush();
						}
					bwr.close();
					//sorting the table according to order By clause
					//newTable= OrderByOperation.orderBy(newTable, orderByList, orderByList.size());
					
					// putting the identity of chunk in chunkList
					chunkList.put(chunkCount,newTable);
					
					chunkCount++;
					
				}

				
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		
			
			
			ArrayList<Table> chunksList = new ArrayList<Table>();
			for(int i=0;i<chunkList.size();i++){
				chunksList.add(chunkList.get(i));
			}
			
			//System.out.println(chunksList.size());
			return chunksList;
			
			
		}
		
		// Method to compare the tuples and flush in the finalChunk
		// returns the number of table for which value was flushed in finalChunk
	public static int mergeAndFlush(String tuple1,String tuple2,List orderByList,int size,Table finalChunk,BufferedWriter bwr) throws IOException{
		//FileWriter fwr = null;
		//BufferedWriter bwr = null;
		
		/*System.out.println("=========");
		System.out.println(tuple1);
		System.out.println(tuple2);
		System.out.println("=========");*/
		//System.out.println("in merge flush");
		
		
		//fwr = new FileWriter(new File(finalChunk.tableDatFilePath));
		//bwr = new BufferedWriter(fwr);
		//this is the identifier of the table which is to be returned
		int tableNo = 0;
		
		
		// contains the order by attribute list
		String  orderByArr[] = new String[size];
				
		int i=0;
				
		for(Object o:orderByList){
				orderByArr[i] = o.toString();
				i++;
		}
		
		int []index = new int[orderByArr.length];
		
		// order denotes if order by is desc or not
		boolean []order = new boolean[orderByArr.length];
		
		for (int j = 0; j < orderByArr.length; j++) {
			String column = orderByArr[j];
			String colArr[]=column.split(" ");
			
			
			index[j] =  finalChunk.columnIndexMap.get(colArr[0]);
			if(colArr.length>1){
				if(colArr[1].equalsIgnoreCase("desc")){

					order[j] = true;
				}
			}
			
		}
		
		String[] tupArr1 = tuple1.split("\\|");
		String[] tupArr2 = tuple2.split("\\|");
		
		for (int j = 0; j < orderByArr.length; j++) {
			String colFromTup1 = tupArr1[index[j]];
			String colFromTup2 = tupArr2[index[j]];
			//System.out.println(finalChunk.columnIndexMap.size());
			String type = finalChunk.columnDescriptionList.get(finalChunk.columnIndexMap.get(finalChunk.columnDescriptionList.get(index[j]).getColumnName())).getColDataType().toString();
			//System.out.println(type);
			//System.out.println(colFromTup1);
			//System.out.println(colFromTup2);
			
			// Order By Desc
			if(order[j])
			{
			
				if(type.equalsIgnoreCase("string") || type.equalsIgnoreCase("char")){
					
					if(colFromTup1.compareTo(colFromTup2)==1){
						
						bwr.write(tuple1);
						bwr.write("\n");
						bwr.flush();
						tableNo = 1;
						break;
						
					}
					else if(colFromTup1.compareTo(colFromTup2)==-1){
						bwr.write(tuple2);
						bwr.write("\n");
						bwr.flush();
						tableNo = 2;
						break;
						
					}
					else if(colFromTup1.compareTo(colFromTup2)==0){
						if(j+1<orderByArr.length){
							continue;
						}
						else{
							bwr.write(tuple1);
							bwr.write("\n");
							bwr.flush();
							tableNo = 1;
							break;
						}
						
						
					}
					
					
				}
				else if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("decimal")){
					
					if(Integer.parseInt(colFromTup1) > Integer.parseInt(colFromTup2)){
						bwr.write(tuple1);
						bwr.write("\n");
						bwr.flush();
						tableNo = 1;
						break;
						
					}
					else if(Integer.parseInt(colFromTup1) < Integer.parseInt(colFromTup2)){
						bwr.write(tuple2);
						bwr.write("\n");
						bwr.flush();
						tableNo = 2;
						break;
						
					}
					else if(Integer.parseInt(colFromTup1) == Integer.parseInt(colFromTup2)){
						if(j+1<orderByArr.length){
							continue;
						}
						else{
							bwr.write(tuple1);
							bwr.write("\n");
							bwr.flush();
							tableNo = 1;
							break;
						}
						
					}
				}
				else if(type.equalsIgnoreCase("date")){
					
					return 0;
				}
				
			}
			
			// order By Asc
			else{

				if(type.equalsIgnoreCase("string") || type.equalsIgnoreCase("char")){
					
					
					if(colFromTup2.compareTo(colFromTup1)==1){
						
						bwr.write(tuple1);
						bwr.write("\n");
						bwr.flush();
						tableNo = 1;
						break;
						
					}
					else if(colFromTup2.compareTo(colFromTup1)==-1){
						bwr.write(tuple2);
						bwr.write("\n");
						bwr.flush();
						tableNo = 2;
						break;
						
					}
					else if(colFromTup1.compareTo(colFromTup2)==0){
						if(j+1<orderByArr.length){
							continue;
						}
						else{
							bwr.write(tuple1);
							bwr.write("\n");
							bwr.flush();
							tableNo = 1;
							break;
						}
						
						
					}
					
					
				}
					else if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("decimal")){
						
						if(Integer.parseInt(colFromTup2) > Integer.parseInt(colFromTup1)){
							bwr.write(tuple1);
							bwr.write("\n");
							bwr.flush();
							tableNo = 1;
							break;
							
						}
						else if(Integer.parseInt(colFromTup2) < Integer.parseInt(colFromTup1)){
							bwr.write(tuple2);
							bwr.write("\n");
							bwr.flush();
							tableNo = 2;
							break;
							
						}
						else if(Integer.parseInt(colFromTup1) == Integer.parseInt(colFromTup2)){
							if(j+1<orderByArr.length){
								continue;
							}
							else{
								bwr.write(tuple1);
								bwr.write("\n");
								bwr.flush();
								tableNo = 1;
								break;
							}
							
						}
					}
					else if(type.equalsIgnoreCase("date")){
						
						return 0;
					}

				
			}
			
	}
		return tableNo;
		
	}
	
	// This is a recursive method which merges the chunks in the chunk list 
	public static Table merging(ArrayList<Table> chunkList, List orderByList,int size) throws IOException{

		//System.out.println(chunkList.size());

		if(chunkList.size()>0){
		
		MergingComparator comparator = new MergingComparator(chunkList.get(0), orderByList, size);
		PriorityQueue<String> queueOfTuples = new PriorityQueue<String>(chunkList.size(), comparator);
		
		for(int i=0;i<chunkList.size();i++){
			
			queueOfTuples.add(chunkList.get(i).returnTuple()+"|"+i);
			
			
		}
		
		
		Table finalTable = new Table("final_table",chunkList.get(0).noOfColumns,new File(chunkList.get(0).tableDataDirectoryPath+"\\"+"final_table"),chunkList.get(0).tableDataDirectoryPath);
		
		FileWriter fwr = new FileWriter(finalTable.tableFilePath);
		BufferedWriter bwr = new BufferedWriter(fwr);
		
		while(queueOfTuples.size()>0){
			//System.out.println(queueOfTuples.peek());
			int lstIndexofPipe = queueOfTuples.peek().lastIndexOf("|");
			//System.out.println(lstIndexofPipe);
			//System.out.println(queueOfTuples.peek().length()-1);
			int tableNo = Integer.parseInt(queueOfTuples.peek().substring(lstIndexofPipe+1, queueOfTuples.peek().length()));
			bwr.write(queueOfTuples.poll().substring(0, lstIndexofPipe)+"\n");
			//bwr.newLine();
			//bwr.flush();
			String tuple = null;
			if((tuple = chunkList.get(tableNo).returnTuple()) !=null){
			queueOfTuples.add(tuple+"|"+tableNo);
			}
		}
		bwr.close();
		
		return finalTable;
		
		
	}
		else{
			return null;
		}
		
	}
	
}


class MergingComparator implements Comparator<String>{

	// the values for compare attributes 
	int index = 0;
	boolean order = false;
	String type = null;
	
	// Values denoting Table, orderByList and size of the list
	Table table;
	List orderByList;
	int size;
	
	//  arrays containing the values to compare
	int []indexArr= null;
	boolean []orderArr = null;
	String typeArr[] = null;
	
	// count for comparator
	int orderArrCount = 0;
	
	public MergingComparator(Table table, List orderByList,int size) {
		this.table = table;
		this.orderByList = orderByList;
		this.size = size;	
		orderArrCount = orderByList.size()-1;
		populateOrderingAttributes();
		moveToNextOrdering();
		
	}
	
	// method returns true if next order clause is available
	private boolean moveToNextOrdering(){
		
		if(orderArrCount >=0){
			index =  indexArr[orderArrCount];
			order = orderArr[orderArrCount];
			type = typeArr[orderArrCount];
			orderArrCount--;
			return true;
		}
		else{
			
			return false;
		}
			
	}
	
	
	private void populateOrderingAttributes(){
		
		String  orderByArr[] = new String[size];
		
		int i=0;
		
		
		for(Object o:orderByList){
			//System.out.println(o.toString());
			orderByArr[i] = o.toString();
			i++;
		}
//		System.out.println(table.columnIndexMap);
		typeArr = new String[size];
		
						
			indexArr = new int[orderByArr.length];
			
			orderArr = new boolean[orderByArr.length];
			//System.out.println(orderArr.length);
			for (int j = 0; j < orderByArr.length; j++) {
				String column = orderByArr[j];
				String colArr[]=column.split(" ");
			//System.out.println(colArr.length);
				//System.out.println(table.columnIndexMap.get(colArr[0].trim()));
				
					
					indexArr[j] =  table.columnIndexMap.get(colArr[0]);
					typeArr[j] = table.columnDescriptionList.get(table.columnIndexMap.get(table.columnDescriptionList.get(indexArr[j]).getColumnName())).getColDataType().toString();
					if(colArr.length>1){
						if(colArr[1].equalsIgnoreCase("desc")){

							orderArr[j] = true;
						}
					}
				
			}
		
	}
	
	
	@Override
	public int compare(String o1, String o2) {
		String s1[] = o1.split("\\|");
		String s2[] = o2.split("\\|");
		
		
		if(type.equalsIgnoreCase("char")|| type.equalsIgnoreCase("VARCHAR")||type.equalsIgnoreCase("String")){
		
			if(s1[index].equals(s2[index])){
					if(moveToNextOrdering()){
						compare(o1, o2);
					}
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
		else if(type.equalsIgnoreCase("int")|| type.equalsIgnoreCase("decimal")){
			
			if(s1[index]==s2[index]){
				if(moveToNextOrdering()){
					compare(o1, o2);
				}
				return 0;
			}
			
			else {
				int x =0;
				if(order==false){
					
					if(Double.parseDouble(s1[index])>Double.parseDouble(s2[index])){
						x = 1;
					}
					else if(Double.parseDouble(s1[index])<Double.parseDouble(s2[index])){
						x=-1;
					}
					else{
						x=0;
					}
					
				}
				else{
					if(Double.parseDouble(s1[index])>Double.parseDouble(s2[index])){
						x = -1;
					}
					else if(Double.parseDouble(s1[index])<Double.parseDouble(s2[index])){
						x=1;
					}
					else{
						x=0;
					}
				}
				
				return x;
			}
		}
	
		return 0;
		
	}
	
}