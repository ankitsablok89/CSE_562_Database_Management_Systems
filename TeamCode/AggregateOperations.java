package edu.buffalo.cse562;

import java.io.File;
import java.util.List;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.io.FileWriter;
import java.util.ArrayList;
import java.io.IOException;
import java.math.BigDecimal;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

public class AggregateOperations 
{

	//Function to return column in an aggregate function
	public String RetColumnName(Table newTable,String[] selectList,String groupBy)
	{

		
		String columnName=null;
		String aggregateFunc=null;
		String concat=null;
		for(int i=0;i<selectList.length;i++)
		{
			if(selectList[i].contains("SUM")||selectList[i].contains("MIN")||selectList[i].contains("MAX")||selectList[i].contains("AVG")||
					selectList[i].contains("sum")||selectList[i].contains("min")||selectList[i].contains("max")||selectList[i].contains("avg")||
					selectList[i].contains("count")||selectList[i].contains("COUNT"))
			{
				aggregateFunc=selectList[i].substring(0,selectList[i].indexOf("("));
				columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].indexOf(")"));
			}	    		
		}
		concat=aggregateFunc+","+columnName;
		return concat;

	}

	//Function to calculate the Group BY Clause and store the values in a Hash Map

	public HashMap<String,Double> getGroupBy(Table newTable,String groupBy,String columnName,int columnNo,String aggregateFunc) throws IOException
	{
		


		double columnAdd=0;
		
		int flag=0;
		//Creating a HashMap to Store Group<Key,Value> Pair
		HashMap<String, Double> groupByMap=new HashMap<String,Double>();
		HashMap<String,String> countMap=new HashMap<String,String>();

		String editedGroupBy=groupBy.substring(1,groupBy.lastIndexOf("]"));

		String[] checkGroupBy=editedGroupBy.split(",");
		String[] groupBycolumnName = new String[10];

		for(int i=0;i<checkGroupBy.length;i++)
		{
			groupBycolumnName[i]=checkGroupBy[i];
		}

		String canString=newTable.returnTuple();
		
		while(canString!=null)
		{
			String MatchedColumn=new String();

			String[] tupleList=canString.split("\\|");	
			if(checkGroupBy.length==1)
			{
				MatchedColumn=tupleList[newTable.columnIndexMap.get(groupBycolumnName[0].trim())];
			}
			else	

			{	
				MatchedColumn=tupleList[newTable.columnIndexMap.get(groupBycolumnName[0].trim())];

				for(int i=1;i<checkGroupBy.length;i++)
				{
					MatchedColumn=MatchedColumn+"|"+tupleList[newTable.columnIndexMap.get(groupBycolumnName[i].trim())];

				}

			}
			canString=newTable.returnTuple();	

			//Column inside the Group By Clause

			//Checks whether the function is SUM"
			if(aggregateFunc.trim().contains("SUM")||aggregateFunc.trim().contains("sum"))
			{	
				
				columnAdd=(Double.parseDouble(tupleList[(newTable.columnIndexMap.get(columnName))]));
				if(groupByMap.containsKey(MatchedColumn)){ 
					groupByMap.put(MatchedColumn,groupByMap.get(MatchedColumn)+columnAdd);
				}
				else{
					groupByMap.put(MatchedColumn,columnAdd);
				}
			}

			//check whether the aggregate function is MIN
			else if(aggregateFunc.trim().contains("MIN")||aggregateFunc.trim().contains("min"))
			{
				columnAdd=Double.parseDouble(tupleList[(newTable.columnIndexMap.get(columnName))]);
				double comp=columnAdd;

				if(groupByMap.containsKey(MatchedColumn))
				{
					if(groupByMap.get(MatchedColumn)>comp)
					{
						groupByMap.put(MatchedColumn,comp);
					}
				}
				else 
				{
					groupByMap.put(MatchedColumn,comp);
				}
			}

			//check whether the aggregate function is MAX

			else if(aggregateFunc.trim().contains("MAX")||aggregateFunc.trim().contains("max"))
			{
				columnAdd=Double.parseDouble(tupleList[(newTable.columnIndexMap.get(columnName))]);
				double comp=columnAdd;
				if(groupByMap.containsKey(MatchedColumn))
				{
					if(groupByMap.get(MatchedColumn)<comp)
					{
						groupByMap.put(MatchedColumn,comp);
					}
				}
				else 
				{
					groupByMap.put(MatchedColumn,comp);
				}
			}
			else if(aggregateFunc.trim().contains("COUNT")||aggregateFunc.trim().contains("count")||aggregateFunc.trim().contains("distinct")||aggregateFunc.trim().contains("DISTINCT"))
			{
				if(aggregateFunc.trim().contains("distinct")||aggregateFunc.trim().contains("DISTINCT"))
				{
					
					flag=1;
					String column=tupleList[(newTable.columnIndexMap.get(columnName))];
					//System.out.println(column);
					if(countMap.containsKey(MatchedColumn))
					{

						countMap.put(MatchedColumn,countMap.get(MatchedColumn)+"|"+column);
					}
					else
					{

						countMap.put(MatchedColumn,column);
					}
				}
				else
				{

					if(groupByMap.containsKey(MatchedColumn))
					{
						groupByMap.put(MatchedColumn,groupByMap.get(MatchedColumn)+1);


					}
					else 
					{
						groupByMap.put(MatchedColumn,1.0);


					}
				}
			}
		}


		if(flag==1)
		{
			Iterator iter=countMap.keySet().iterator();

			while(iter.hasNext())
			{
				HashSet<String> DupSet=new HashSet<String>();
				String key =iter.next().toString();  
				String value =countMap.get(key).toString(); 
				String[] StrArr=value.split("\\|");
				for(String s:StrArr)
				{
					DupSet.add(s);
				}
				groupByMap.put(key,Double.valueOf(DupSet.size()));
			}

		}
		return groupByMap;
	}


	//Function to calculate the Group BY Clause and store the values in a Hash Map

	public HashMap<String,Double> getPostCal(Table newTable,String groupBy,String aggregateFunc,String posixExpression) throws IOException, ParseException
	{
		
		//Creating a HashMap to Store Group<Key,Value> Pair
		HashMap<String, Double> groupByMap=new HashMap<String,Double>();
		String canString=null;

		String MatchedColumn=new String();

		//String array to store all the values in the column
		String arr[]=new String[10];
		String[] tupleList=null;
		//postfix contains a list of all columns in a post fix form
		ArrayList<String> arrList = WhereOperation.braceExp(posixExpression);
		String array[] = new String[arrList.size()];
		array = arrList.toArray(array);


		//String[] postFix=SelectionOperation.convertToPos(posixExpression.split(" "));
		String[] postFix=WhereOperation.convertToPos(array);

		String editedGroupBy=groupBy.substring(1,groupBy.lastIndexOf("]"));

		String[] checkGroupBy=editedGroupBy.split(",");
		String[] groupBycolumnName = new String[10];

		for(int i=0;i<checkGroupBy.length;i++)
		{
			groupBycolumnName[i]=checkGroupBy[i];
		}

		canString=newTable.returnTuple();
		while(canString!=null)
		{ 
			tupleList=canString.split("\\|");

			if(checkGroupBy.length==1)
			{
				MatchedColumn=tupleList[newTable.columnIndexMap.get(groupBycolumnName[0].trim())];
			}
			else
			{					
				MatchedColumn=tupleList[newTable.columnIndexMap.get(groupBycolumnName[0].trim())];

				for(int i=1;i<checkGroupBy.length;i++)
				{
					MatchedColumn=MatchedColumn+"|"+tupleList[newTable.columnIndexMap.get(groupBycolumnName[i].trim())];

				}
			}

			for(int k=0;k<postFix.length;k++)
			{
				if(newTable.columnIndexMap.containsKey(postFix[k]))
				{
					arr[k]=tupleList[newTable.columnIndexMap.get(postFix[k])];
				}
				else
				{
					arr[k]=postFix[k];
				}
			}


			if(aggregateFunc.equalsIgnoreCase("SUM"))
			{
				if(groupByMap.containsKey(MatchedColumn))	
				{
					double result=(WhereOperation.evaluate(arr));
					groupByMap.put(MatchedColumn,groupByMap.get(MatchedColumn)+result);

				}
				else
				{
					double result=WhereOperation.evaluate(arr);
					groupByMap.put(MatchedColumn,(double)result);
				}


			}
			canString=newTable.returnTuple();


		}
		return groupByMap;
	}

	public double sum(Table newTable,String[] selectList,String columnName) throws IOException
	{

		double sum=0;
		String[] pos=columnName.split(" ");
		String[] postFix=WhereOperation.convertToPos(pos);
		String[] arr=new String[postFix.length];
		String canString = null;
		String[] tupleList=null;

		sum=0;
		canString=newTable.returnTuple();

		while(canString!=null)
		{	tupleList=canString.split("\\|");

		for(int k=0;k<postFix.length;k++)
		{
			if(newTable.columnIndexMap.containsKey(postFix[k]))
			{
				arr[k]=tupleList[newTable.columnIndexMap.get(postFix[k])];
			}
			else
			{
				arr[k]=postFix[k];
			}
		}

		double val=WhereOperation.evaluate(arr);
		sum=sum+val;
		canString=newTable.returnTuple(); 
		}
		return sum;
	}

	//Aggregate Function to get the Sum
	//Returns a table containing the column names(Group BY ones) and the resultant aggregate Function

	@SuppressWarnings("resource")
	public Table getAggregate(Table newTable,String[] selectList,String check,List orderByList) throws IOException, ParseException, InterruptedException
	{	
		Table newTable1=null;
		//System.out.println("getAggregate Table: " + newTable.tableFilePath);
		String[] tupleList=null;
		String canString=null;
		HashMap<String,Double> map;
		int count=0;
		Double Sum=0.0;
		Table GroupByTable=null;
		String columnName=selectList[0];
	
		String editedGroupBy=null;
		String[] groupBy=null;
	//	System.out.println("getAggregate check: " + check);
		
		if(!check.contains("NOGroupBy"))
		{
			editedGroupBy=check.substring(1,check.lastIndexOf("]"));
			groupBy=editedGroupBy.split(",");	
			//System.out.println("getAggregate GroupBy:");
			/*for(String s : groupBy)
				System.out.println("getAggregate GroupBy: " + s);*/
		}
		else
		{
			groupBy=check.split			(",");	
			/*for(String s : groupBy)
				System.out.println("getAggregate NoGroupBy: " + s);*/
		}

		for (int i = 0; i < groupBy.length; i++) {
			groupBy[i] = groupBy[i].trim();
			//System.out.println("Trimmed GroupBy:" + groupBy[i]);
		}

		for (int i = 0; i < selectList.length; i++) {
			selectList[i] = selectList[i].trim();
			//System.out.println("getAggregate Trimmed selectListItem : " + selectList[i]);
		}

		ArrayList<Object> arrTuple = null;
		String str =null;
		arrTuple=new ArrayList<Object>();
		double Sum1=0;
		HashMap<String, Double> postMap;
		String line=null;
		
		String arrStr=new String();

		int columnNo=0;
		Double min=0.0;			
		Double max=0.0;
		
		//Initializing the table and the table constructor
		//System.out.println("GroupBy file : " + newTable.tableDataDirectoryPath + System.getProperty("file.separator") + newTable.tableName + "Group.tbl");
		File groupByFile = new File(newTable.tableDataDirectoryPath + System.getProperty("file.separator") + newTable.tableName + "Group.tbl");
		
		// check if the group by file exists or not, if not then create it
		if(!groupByFile.exists()){
			//System.out.println("Creating GroupBy file");
			groupByFile.createNewFile();
		}
		
		// this is the groupBy table corresponding to the group by file created above
		GroupByTable = new Table(newTable.tableName + "Group", newTable.noOfColumns, groupByFile,newTable.tableDataDirectoryPath);	
		
		//System.out.println(newTable.columnIndexMap);
		if(!groupBy[0].contains("NOGroupBy"))
		{
		for(int i=0;i<groupBy.length;i++){
			
			ColumnDefinition colDescTypePair=new ColumnDefinition();
		    ColDataType col=new ColDataType();
			colDescTypePair.setColumnName(groupBy[i]);
			String type=newTable.columnDescriptionList.get(newTable.columnIndexMap.get(groupBy[i])).getColDataType().getDataType().toString();

			col.setDataType(type);
			colDescTypePair.setColDataType(col);
            GroupByTable.columnDescriptionList.add(colDescTypePair);
		}
		}
		//System.out.println("Size"+GroupByTable.columnDescriptionList.size());
		//Checks if the groupBycolumn is null
		if(groupBy[0].contains("NOGroupBy")){
			
			for(int i=0;i<selectList.length;i++)
			{	
				if(selectList[i].contains("SUM")||selectList[i].contains("AVG")||selectList[i].contains("sum")||
				   selectList[i].contains("avg")||selectList[i].contains("min")||selectList[i].contains("max")
				   ||selectList[i].contains("COUNT")|| selectList[i].contains("count")||selectList[i].contains("MIN")
				   ||selectList[i].contains("MAX")){
					
					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
				}	
				
				if(columnName.trim().contains("*")||columnName.trim().contains("-")||columnName.contains("+")||columnName.contains("/"))
				{

					Sum1=sum(newTable,selectList,columnName);
					arrStr=arrStr+Sum1+"|";
					String FinalSum=arrStr.substring(0,arrStr.lastIndexOf("|"));

					System.out.println(FinalSum);
				}
				
				if(selectList[i].contains("SUM")||selectList[i].contains("sum")&&
						!columnName.contains("*")&&!columnName.contains("-")&&!columnName.contains("+"))
				{
					
					columnNo=newTable.columnIndexMap.get(columnName.trim());
					canString=newTable.returnTuple();

					while(canString!=null)
					{							

						tupleList=canString.split("\\|");
						Sum=Sum+Double.parseDouble(tupleList[columnNo]);
						canString=newTable.returnTuple();

					}
					FileWriter fw = new FileWriter(new File(newTable.tableDataDirectoryPath + System.getProperty("file.separator") + newTable.tableName + "Group.tbl"),true);
					BufferedWriter bw = new BufferedWriter(fw);
					
					bw.write(BigDecimal.valueOf(Sum).toString());
					bw.close();
				}

				if(selectList[i].contains("COUNT(")||selectList[i].contains("COUNT(*)")||selectList[i].contains("AVG(")||selectList[i].contains("count(")||selectList[i].contains("count(*)")||selectList[i].contains("avg("))
				{
					
					canString=newTable.returnTuple();
					tupleList=canString.split("\\|");
					columnNo=newTable.columnIndexMap.get(columnName.trim());
                        
					Integer count1=0;
					while(canString!=null)

					{
						count1++;

						canString=newTable.returnTuple();
					}
					FileWriter fw = new FileWriter(new File(newTable.tableDataDirectoryPath + System.getProperty("file.separator") + newTable.tableName + "Group.tbl"),true);
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(count1.toString());
					//fw.close();
					bw.close();
					
				}

				if(selectList[i].contains("AVG")||selectList[i].contains("avg")){

					Float avg=((float)Sum1/(float)count);
					FileWriter fw = new FileWriter(new File(newTable.tableDataDirectoryPath + System.getProperty("file.separator") + newTable.tableName + "Group.tbl"),true);
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(avg.toString());
				

				}

				if(selectList[i].contains("MIN")||selectList[i].contains("min"))
				{
					canString=newTable.returnTuple();
					tupleList=canString.split("\\|");
					columnNo=newTable.columnIndexMap.get(columnName);
					min=Double.parseDouble(tupleList[columnNo]);

					while(canString!=null)
					{			

						tupleList=canString.split("\\|");

						if(min>=Double.parseDouble(tupleList[columnNo])){

							min=Double.parseDouble(tupleList[columnNo]);
						}


						canString=newTable.returnTuple();


					}
					FileWriter fw = new FileWriter(new File(newTable.tableDataDirectoryPath + System.getProperty("file.separator") + newTable.tableName + "Group.tbl"),true);
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(min.toString());
					bw.close();
					
				}


				if(selectList[i].contains("MAX")||selectList[i].contains("max"))
				{
					canString=newTable.returnTuple();
					tupleList=canString.split("\\|");
					columnNo=newTable.columnIndexMap.get(columnName);
					max=Double.parseDouble(tupleList[columnNo]);

					while(canString!=null)
					{			
						tupleList=canString.split("\\|");
                        
						if(max<=Double.parseDouble(tupleList[columnNo]))
						{
							max=Double.parseDouble(tupleList[columnNo]);

						}
						canString=newTable.returnTuple();
					}
					FileWriter fw = new FileWriter(new File(newTable.tableDataDirectoryPath + System.getProperty("file.separator") + newTable.tableName + "Group.tbl"),true);
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(max.toString());
					bw.close();
				}
			}
			return GroupByTable;

		}

		//checks for the group by items. enters if there is a group By clause
		else if(!check.equalsIgnoreCase("NOGroupBy")){	
				
			ArrayList<String> arrList=new ArrayList<String>();
			
		    
			for(int i=0;i<selectList.length;i++)
			{	
				
			
				arrList.add((selectList[i]));
				
				if(selectList[i].contains("SUM")||selectList[i].contains("sum"))
				{	
					//System.out.println("getAggregate in SUM function");
					if(selectList[i].contains("AS")||selectList[i].contains("as"))
					{
						str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
					    ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(str);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
		                GroupByTable.columnDescriptionList.add(colDescTypePair);

					}
					else
					{
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
					    ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(selectList[i]);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
		                GroupByTable.columnDescriptionList.add(colDescTypePair);

					}
					
					// checks if there is any calculation needed inside the Sum Function
					if(columnName.contains("*")||columnName.contains("-")||columnName.contains("+")||columnName.contains("/")||columnName.contains("("))
					{
						postMap=getPostCal(newTable,check,"SUM",columnName);
						Iterator postMapiterator = postMap.keySet().iterator();
						
						FileReader fr1 = new FileReader(GroupByTable.tableFilePath);
						BufferedReader br1 = new BufferedReader(fr1);
						line=br1.readLine();
						arrTuple.clear();
						
						if(line != null)
						{

							while (postMapiterator.hasNext()&&line!=null) 
							{  
								String key = postMapiterator.next().toString();  
								BigDecimal value = BigDecimal.valueOf(postMap.get(key));  

								arrTuple.add(line+"|"+value);
								line = br1.readLine();
								}
							br1.close();

							//if(groupByFile.delete()){}
							FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
							BufferedWriter bw = new BufferedWriter(fw);
							
							for(Object s:arrTuple){
								bw.write(s.toString()+"\n");
							}
						bw.close();
						}
						else
						{					
							FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
							BufferedWriter bw = new BufferedWriter(fw);
							
							while (postMapiterator.hasNext()) 
							{  	
								String key = postMapiterator.next().toString();
								BigDecimal value = BigDecimal.valueOf(postMap.get(key));  
								bw.write(key+"|"+value + "\n");
							}
							bw.close();
						}
					}

					else if(!columnName.contains("*")||columnName.contains("-")||columnName.contains("+")
							||columnName.contains("/")){  
						//Storing the aggregate function
						/*System.out.println(columnName);
						System.out.println("abhinav");
						System.out.println(newTable.columnIndexMap);*/
						columnNo=newTable.columnIndexMap.get(columnName);
						String aggString=selectList[i].substring(0,selectList[i].indexOf("("));
						map=getGroupBy(newTable,check,columnName, columnNo, aggString);
						@SuppressWarnings("rawtypes")
						Iterator mapiterator = map.keySet().iterator();
						
						FileReader fr1 = new FileReader(GroupByTable.tableFilePath);
						BufferedReader br1 = new BufferedReader(fr1);
						
						line=br1.readLine();
						arrTuple.clear();
						
						if(line!=null)
						{
							while (mapiterator.hasNext()&&line!=null) { 
								String key = mapiterator.next().toString();  
								BigDecimal value = BigDecimal.valueOf (map.get(key));  

								arrTuple.add(line+"|"+value);
								line = br1.readLine();
							}
							br1.close();

							/*if(groupByFile.delete()){}*/
							FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
							BufferedWriter bw = new BufferedWriter(fw);
							for(Object s:arrTuple)
							{
								bw.write(s.toString()+"\n");
							}
							bw.close();
						}

						else
						{				
							FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
							BufferedWriter bw = new BufferedWriter(fw);
							while (mapiterator.hasNext()) 
							{  	
								String key = mapiterator.next().toString();
								BigDecimal value = BigDecimal.valueOf( map.get(key)); 
								bw.write(key+"|"+value.toString()+"\n");
							

							}
						bw.close();
						}

					}
				}

				if(selectList[i].contains("COUNT(")||selectList[i].contains("count(")){	
				
					HashMap<String,Double> CountMap=new HashMap<String, Double>();
					
					if(selectList[i].contains(" AS ")||selectList[i].contains(" as "))
					{
						if(selectList[i].contains("DISTINCT")||selectList[i].contains("distinct"))
						{
							str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
							columnName=selectList[i].substring(selectList[i].indexOf(" ")+1,selectList[i].lastIndexOf(")"));
							ColumnDefinition colDescTypePair=new ColumnDefinition();
						    ColDataType col=new ColDataType();
							colDescTypePair.setColumnName(str);
							col.setDataType("DECIMAL");
							colDescTypePair.setColDataType(col);
			                GroupByTable.columnDescriptionList.add(colDescTypePair);

						}
						else
						{
						str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
					    ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(str);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
		                GroupByTable.columnDescriptionList.add(colDescTypePair);
						if(columnName.equals("*"));
						{
							columnName=selectList[0];
						}
						columnNo=newTable.columnIndexMap.get(columnName.trim());
						}
					}
					else
					{
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						if(columnName.contains("DISTINCT")||columnName.contains("distinct"))
						{
							
							columnName=columnName.substring(columnName.indexOf(" "),columnName.length());

						}
						ColumnDefinition colDescTypePair=new ColumnDefinition();
					    ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(selectList[i]);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
		                GroupByTable.columnDescriptionList.add(colDescTypePair);
						if(columnName.equals("*"))
						{				
							columnName=selectList[0];
						}
						columnNo=newTable.columnIndexMap.get(columnName.trim());
					}
					if(selectList[i].contains("distinct")||selectList[i].contains("DISTINCT"))
					{
						//System.out.println("column Name="+columnName);
						CountMap=getGroupBy(newTable,check, columnName.trim(), columnNo,"distinct");
					}
					else
					{
						CountMap=getGroupBy(newTable,check, columnName, columnNo,"COUNT");
					}
					
					@SuppressWarnings("rawtypes")
					Iterator countIterator = CountMap.keySet().iterator();  

					//Iterating over the HashMap
					FileReader fr1=new FileReader(GroupByTable.tableFilePath);
					BufferedReader br1=new BufferedReader(fr1);

					line=br1.readLine();
					arrTuple.clear();
					if(line!=null){
						while (countIterator.hasNext()&&line!=null) 
						{  
							String key = countIterator.next().toString();

							Integer val = CountMap.get(key).intValue();
							String value = val.toString();

							arrTuple.add(line+"|"+value);
							line=br1.readLine();
						}
						br1.close();
					
						FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
						BufferedWriter bw = new BufferedWriter(fw);
						for(Object s:arrTuple)
						{
							bw.write(s.toString()+"\n");
							bw.flush();

						}
						bw.close();
					} else{
						FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
						BufferedWriter bw = new BufferedWriter(fw);
						while (countIterator.hasNext()) 
						{ 					
							String key = countIterator.next().toString(); 
							Integer v = CountMap.get(key).intValue();
							String value = v.toString();
							
							bw.write(key.toString()+"|"+value.toString()+"\n");
							bw.flush();
					
						}
						bw.close();
					}

				}

				if(selectList[i].contains("AVG")||selectList[i].contains("avg")){

					if(selectList[i].contains("AS")||selectList[i].contains("as")){
						str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						columnNo=newTable.columnIndexMap.get(columnName.trim());
						ColumnDefinition colDescTypePair=new ColumnDefinition();
					    ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(str);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
		                GroupByTable.columnDescriptionList.add(colDescTypePair);
					}else{

						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						columnNo=newTable.columnIndexMap.get(columnName.trim());
						ColumnDefinition colDescTypePair=new ColumnDefinition();
					    ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(columnName.trim());
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
		                GroupByTable.columnDescriptionList.add(colDescTypePair);
					}

					HashMap<String, Double> SumMap=getGroupBy(newTable, check, columnName, columnNo,"SUM");
					HashMap<String, Double> countMap=getGroupBy(newTable, check, columnName, columnNo,"COUNT");

					HashMap<String,BigDecimal> avgMap=new HashMap<String,BigDecimal>();

					@SuppressWarnings("rawtypes")
					Iterator AvgIterator = SumMap.keySet().iterator();  

					while (AvgIterator.hasNext()) 
					{  
						String key = AvgIterator.next().toString();  
						BigDecimal value = BigDecimal.valueOf(SumMap.get(key)/countMap.get(key));
						avgMap.put(key, value);

					}
					@SuppressWarnings("rawtypes")
					Iterator Averageiterator = avgMap.keySet().iterator();  
					FileReader fr1 =new FileReader(groupByFile);
					BufferedReader br1 =new BufferedReader(fr1);						
					line=br1.readLine();

					String[] parts=null;
					arrTuple.clear();

					if(line!=null)
					{				

						while (Averageiterator.hasNext()&&line!=null) 
						{ 
							parts=line.split("\\|");
							String str1=parts[0];

							for(int j=1;j<groupBy.length;j++)
							{
								str1=str1+"|"+parts[j];
							}

							BigDecimal value=avgMap.get(str1);
							//float value=avgMap.get(superKey);


							arrTuple.add(line+"|"+value);
							line=br1.readLine();
						}
						
						FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
						BufferedWriter bw = new BufferedWriter(fw);
						for(Object s:arrTuple)
						{
							bw.write(s.toString()+"\n");
						}
						bw.close();
					}

					else if(line==null)
					{				

						FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
						BufferedWriter bw = new BufferedWriter(fw);
						
						String superKey=null;
						while (Averageiterator.hasNext()) 
						{  
							String key = Averageiterator.next().toString();  

							BigDecimal  value=avgMap.get(key);

							bw.write(key+"|"+value);
							bw.write("\n");
						}	
						bw.close();

					}

				}

				if(selectList[i].contains("MIN")||selectList[i].contains("min"))
				{

					if(selectList[i].contains("AS")||selectList[i].contains("as"))
					{
						str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
					    ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(str);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
		                GroupByTable.columnDescriptionList.add(colDescTypePair);
						columnNo=newTable.columnIndexMap.get(columnName.trim());
					}
					else
					{
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
					    ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(columnName.trim());
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
		                GroupByTable.columnDescriptionList.add(colDescTypePair);
						columnNo=newTable.columnIndexMap.get(columnName.trim());
					}

					HashMap<String,Double> MinMap=getGroupBy(newTable,check, columnName, columnNo,"MIN");
					@SuppressWarnings("rawtypes")
					Iterator countIterator = MinMap.keySet().iterator();  
					//Iterating over the HashMap
					FileReader fr1 =new FileReader(GroupByTable.tableFilePath);
					BufferedReader br1 =new BufferedReader(fr1);

					line=br1.readLine();
					arrTuple.clear();
					if(line!=null)
					{
						while (countIterator.hasNext()&&line!=null) 
						{  
							String key = countIterator.next().toString();

						    Double  v = MinMap.get(key);
							String value = v.toString();

							arrTuple.add(line+"|"+value);
							line=br1.readLine();
						}
						br1.close();
						
						FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
						BufferedWriter bw = new BufferedWriter(fw);
						
						for(Object s:arrTuple){
							bw.write(s.toString()+"\n");
						}
						bw.close();

					}
					else
					{
						FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
						BufferedWriter bw = new BufferedWriter(fw);
						
						while (countIterator.hasNext()) 
						{  
							String key = countIterator.next().toString(); 
							Double v = MinMap.get(key);
							double value = v;

							bw.write(key+"|"+value);
							bw.write("\n");

						}
						bw.close();
					}


				}

				if(selectList[i].contains("MAX")||selectList[i].contains("max"))
				{
					if(selectList[i].contains("AS")||selectList[i].contains("as"))
					{
						str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
					    ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(str);
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
		                GroupByTable.columnDescriptionList.add(colDescTypePair);
						columnNo=newTable.columnIndexMap.get(columnName.trim());
					}
					else
					{
						columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
						ColumnDefinition colDescTypePair=new ColumnDefinition();
					    ColDataType col=new ColDataType();
						colDescTypePair.setColumnName(columnName.trim());
						col.setDataType("DECIMAL");
						colDescTypePair.setColDataType(col);
		                GroupByTable.columnDescriptionList.add(colDescTypePair);
						columnNo=newTable.columnIndexMap.get(columnName.trim());
					}

					HashMap<String,Double> MaxMap=getGroupBy(newTable,check, columnName, columnNo,"MAX");
					@SuppressWarnings("rawtypes")
					Iterator countIterator = MaxMap.keySet().iterator();  

					//Iterating over the HashMap
					FileReader fr1 =new FileReader(GroupByTable.tableFilePath);
					BufferedReader br1 =new BufferedReader(fr1);

					line=br1.readLine();
					arrTuple.clear();
					if(line!=null)
					{
						while (countIterator.hasNext()&&line!=null) 
						{  
							String key = countIterator.next().toString();

							Double v = MaxMap.get(key);
							String value = v.toString();

							arrTuple.add(line+"|"+value);
							line=br1.readLine();
						}
						br1.close();

						FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
						BufferedWriter bw = new BufferedWriter(fw);
						
						for(Object s:arrTuple)
						{
							bw.write(s.toString()+"\n");
						}
						bw.close();

					}else{
						
						FileWriter fw = new FileWriter(GroupByTable.tableFilePath);
						BufferedWriter bw = new BufferedWriter(fw);
						
						while (countIterator.hasNext()) 
						{  
							String key = countIterator.next().toString(); 
							Double v = MaxMap.get(key);
							String value = v.toString();

							bw.write(key+"|"+value);
							bw.write("\n");

						}
						bw.close();
					}
				}
			}
			
			GroupByTable.populateColumnIndexMap();
		
			ArrayList<String> arrnew=new ArrayList<String>();

			for(String s:arrList)
			{if(s.contains(" AS ")||s.contains(" as "))
			{
				arrnew.add(s.split(" ")[s.split(" ").length-1]);
			}
			else
			{
				arrnew.add(s);
			}
			}
			
		
			newTable1 = ProjectTableOperation.projectTable(GroupByTable,arrnew,true);
		
		
			if(orderByList != null){
				Table sortedTable= ExternalSort.performExternalMergeSort(newTable1, orderByList);
				return sortedTable;
			}
		}

		return newTable1;
	}

public static void LimitOnTable(Table tableToApplyLimitOn, int tupleLimit) throws IOException {
	
	
	// this is the limit file object
	File limitFile = new File(tableToApplyLimitOn.tableDataDirectoryPath.getAbsolutePath() + System.getProperty("file.separator") + 
							  tableToApplyLimitOn.tableName + "GroupResultLimitTable.tbl");
	
	// this is the Table object corresponding to the file that consists of the limited number of tuples
	/*Table limitTable = new Table(tableToApplyLimitOn.tableName + "GroupResultLimitTable", tableToApplyLimitOn.noOfColumns,
			 					  limitFile, tableToApplyLimitOn.tableDataDirectoryPath);		
	
	FileWriter fwr = new FileWriter(limitTable.tableFilePath, true);
	BufferedWriter bwr = new BufferedWriter(fwr);*/
	
	/*for(int i = 0 ; i < tupleLimit ; ++i){
		// the following is the string that we need to write
		String writeString = limitTable.returnTuple();
		if(writeString.charAt(writeString.length() - 1) == '|')
			bwr.write(writeString + "\n");
		else
			bwr.write(writeString + "|\n");
	}*/
	for(int i=0;i<tupleLimit;i++){
		System.out.println(tableToApplyLimitOn.returnTuple());
	}
	
	//bwr.close();
	
	//return limitTable;
		
	}

}