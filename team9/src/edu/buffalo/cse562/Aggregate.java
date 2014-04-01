package edu.buffalo.cse562;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import javax.swing.plaf.SliderUI;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class Aggregate 
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
		//Creating a HashMap to Store Group<Key,Value> Pair
		HashMap<String, Double> groupByMap=new HashMap<String,Double>();

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
			{					MatchedColumn=tupleList[newTable.columnIndexMap.get(groupBycolumnName[0].trim())];

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
				if(groupByMap.containsKey(MatchedColumn))
				{ 

					groupByMap.put(MatchedColumn, groupByMap.get(MatchedColumn)+columnAdd);
				}
				else
				{
					groupByMap.put(MatchedColumn,columnAdd);
				}
			}

			//check whether the aggregate function is MIN
			else if(aggregateFunc.trim().contains("MIN")||aggregateFunc.trim().contains("min"))
			{
				columnAdd=Integer.parseInt(tupleList[(newTable.columnIndexMap.get(columnName))]);

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
				columnAdd=Integer.parseInt(tupleList[(newTable.columnIndexMap.get(columnName))]);
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

			else if(aggregateFunc.trim().contains("COUNT")||aggregateFunc.trim().contains("count"))
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
		//postfix contains  alist of all columns in a post fix form
		ArrayList<String> arrList = SelectionOperation.braceExp(posixExpression);
		String array[] = new String[arrList.size()];
		array = arrList.toArray(array);


		//String[] postFix=SelectionOperation.convertToPos(posixExpression.split(" "));
		String[] postFix=SelectionOperation.convertToPos(array);

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
					double result=(SelectionOperation.evaluate(arr));
					groupByMap.put(MatchedColumn,groupByMap.get(MatchedColumn)+result);

				}
				else
				{
					double result=SelectionOperation.evaluate(arr);
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
		String[] postFix=SelectionOperation.convertToPos(pos);
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

		double val=SelectionOperation.evaluate(arr);
		sum=sum+val;
		canString=newTable.returnTuple(); 
		}
		return sum;
	}


	//Aggregate Function to get the Sum
	//Returns a table containing the column names(Group BY ones) and the resultant aggregate Function

	@SuppressWarnings("resource")
	public Table getSum(Table newTable,String[] selectList,String check,List orderByList) throws IOException, ParseException
	{	

		HashMap<String,Double> map;
		int count=0;
		double Sum=0;
		Table GroupByTable=null;
		String columnName=null;
		BufferedWriter bwr=null;
		String editedGroupBy=null;
		String[] groupBy=null;
		if(!check.contains("NOGroupBy"))
		{
			editedGroupBy=check.substring(1,check.lastIndexOf("]"));
			groupBy=editedGroupBy.split(",");	
		}
		else
		{
			groupBy=check.split(",");	

		}

		for (int i = 0; i < groupBy.length; i++) {
			groupBy[i] = groupBy[i].trim();
		}
		
		for (int i = 0; i < selectList.length; i++) {
			selectList[i] = selectList[i].trim();
		}
		
		FileWriter fwr=null;
		FileReader fr=null;
		BufferedReader br=null;
		ArrayList<Object> arrTuple = null;
		String str =null;
		arrTuple=new ArrayList<Object>();
		double Sum1=0;
		HashMap<String, Double> postMap;
		String line=null;

		String arrStr=new String();

		int columnNo=0;

		//Initializing the table and the table constructor



		//Checks if the groupBycolumn is null
		if(groupBy[0].trim().contains("NOGroupBy"))
		{	

			for(int i=0;i<selectList.length;i++)
			{		
				if(selectList[i].contains("SUM")||selectList[i].contains("AVG")||selectList[i].contains("sum")||selectList[i].contains("avg")||selectList[i].contains("COUNT")||selectList[i].contains("count"))
				{
					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));

				}	

				if(columnName.contains("*")||columnName.contains("-")||columnName.contains("+")||columnName.contains("/"))
				{

					Sum1=sum(newTable,selectList,columnName);
					arrStr=arrStr+Sum1+"|";
					String FinalSum=arrStr.substring(0,arrStr.lastIndexOf("|"));

					System.out.println(FinalSum);
				}
				if(selectList[i].contains("SUM")||selectList[i].contains("sum")&& !columnName.contains("*")&&!columnName.contains("-")&&!columnName.contains("+"))
				{
					columnNo=newTable.columnIndexMap.get(columnName);
					String canString=newTable.returnTuple();

					while(canString!=null)

					{	

						String[] tupleList=canString.split("\\|");
						Sum=Sum+Double.parseDouble(tupleList[columnNo]);
						canString=newTable.returnTuple();

					}
					System.out.println(Sum);
				}

				if(selectList[i].contains("COUNT(")||selectList[i].contains("COUNT(*)")||selectList[i].contains("AVG(")||selectList[i].contains("count(")||selectList[i].contains("count(*)")||selectList[i].contains("avg("))
				{
					String tuple=newTable.returnTuple();
					while(tuple!=null)

					{	
						count++;

						tuple=newTable.returnTuple();
					}
					System.out.println(count);

				}

				if(selectList[i].contains("AVG")||selectList[i].contains("avg")){

					float avg=((float)Sum1/(float)count);

					System.out.println("Average::"+avg);


				}
			}              

			return GroupByTable;

		}

		//checks for the group by items. enters if there is a group By clause
		else if(!check.equalsIgnoreCase("NOGroupBy"))
		{	
			GroupByTable = new Table("GroupByResultTable",newTable.dataDirectoryPath,newTable.dataDirectoryPath+System.getProperty("file.separator").toString()+newTable.tableName+"GroupBy.dat",selectList.length+groupBy.length);
		fwr = new FileWriter(new File(GroupByTable.tableDatFilePath));
		ArrayList<String> arrList=new ArrayList<String>();
		/* for(int i=0;i<groupBy.length;i++)
		       {Pair colDescTypePair;
				colDescTypePair = new Pair(groupBy[i],"String");
				GroupByTable.columnDescription.add(colDescTypePair);

		       }*/
		
		//System.out.println("HERE");
		for(int i=0;i<selectList.length;i++)
		{		

			arrList.add((selectList[i]));
			//System.out.println(selectList[i]);
			if(!selectList[i].contains("SUM(")&&!selectList[i].contains("sum(")&&!selectList[i].contains("COUNT(")&&!selectList[i].contains("count(")&&
					!selectList[i].contains("AVG(")&&!selectList[i].contains("avg(")&&!selectList[i].contains("Count(*)")&&!selectList[i].contains("count(*)")&&!selectList[i].contains(" AS ")&&!selectList[i].contains(" as "))
			{
				Pair colDescTypePair;
				colDescTypePair = new Pair(selectList[i],"String");
				GroupByTable.columnDescription.add(colDescTypePair);
			}
			if(selectList[i].contains("SUM")||selectList[i].contains("sum"))
			{
				if(selectList[i].contains("AS")||selectList[i].contains("as"))
				{
					str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
					Pair colDescTypePair;
					colDescTypePair = new Pair(str.trim(),"DECIMAL");
					GroupByTable.columnDescription.add(colDescTypePair);

				}
				else
				{
					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
					Pair colDescTypePair;
					colDescTypePair = new Pair(selectList[i].trim(),"DECIMAL");
					GroupByTable.columnDescription.add(colDescTypePair);
				}
				//checks if there is any calculation needed inside the Sum Function
				if(columnName.contains("*")||columnName.contains("-")||columnName.contains("+")||columnName.contains("/")||columnName.contains("("))
				{
					postMap=getPostCal(newTable,check,"SUM",columnName);
					Iterator postMapiterator = postMap.keySet().iterator();

					fr=new FileReader(new File(GroupByTable.tableDatFilePath));
					br=new BufferedReader(fr);
					line=br.readLine();
					arrTuple.clear();
					if(line!=null)
					{

						while (postMapiterator.hasNext()&&line!=null) 
						{  
							String key = postMapiterator.next().toString();  
							String value = postMap.get(key).toString();  
							
							arrTuple.add(line+"|"+value);
							line = br.readLine();
						}
						br.close();

						File filePointer = new File(GroupByTable.tableDatFilePath);

						if(filePointer.delete()){}
						fwr = new FileWriter(filePointer);
						bwr = new BufferedWriter(fwr);

						for(Object s:arrTuple)
						{
							bwr.write(s+"\n");
						}
						bwr.close();
					}
					else
					{
						bwr = new BufferedWriter(fwr);
						while (postMapiterator.hasNext()) 
						{  	
							String key = postMapiterator.next().toString();
							String value = postMap.get(key).toString();  
							bwr.write(key+"|"+value);

							bwr.write("\n");

						}
						bwr.flush();
					}
				}

				else if(!columnName.contains("*")||columnName.contains("-")||columnName.contains("+")||columnName.contains("/"))

				{  
					//Storing the aggregate function
					columnNo=newTable.columnIndexMap.get(columnName);
					String aggString=selectList[i].substring(0,selectList[i].indexOf("("));
					map=getGroupBy(newTable,check,columnName, columnNo, aggString);
					@SuppressWarnings("rawtypes")
					Iterator mapiterator = map.keySet().iterator();

					fr=new FileReader(new File(GroupByTable.tableDatFilePath));
					br=new BufferedReader(fr);
					line=br.readLine();
					arrTuple.clear();
					if(line!=null)
					{

						while (mapiterator.hasNext()&&line!=null) 
						{  
							String key = mapiterator.next().toString();  
							String value = map.get(key).toString();  

							arrTuple.add(line+"|"+value);
							line = br.readLine();
						}
						br.close();

						File filePointer = new File(GroupByTable.tableDatFilePath);

						if(filePointer.delete()){}
						fwr = new FileWriter(filePointer);
						bwr = new BufferedWriter(fwr);

						for(Object s:arrTuple)
						{
							bwr.write(s+"\n");
						}
						bwr.close();
					}

					else
					{
						bwr = new BufferedWriter(fwr);
						while (mapiterator.hasNext()) 
						{  	
							String key = mapiterator.next().toString();
							String value = map.get(key).toString();  
							bwr.write(key+"|"+value);

							bwr.write("\n");

						}
						bwr.flush();
					}



				}
			}

			if(selectList[i].contains("COUNT(")||selectList[i].contains("count("))
			{		
				if(selectList[i].contains("AS")||selectList[i].contains("as"))
				{
					str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
					Pair colDescTypePair;
					colDescTypePair = new Pair(str.trim(),"DECIMAL");
					GroupByTable.columnDescription.add(colDescTypePair);
					if(columnName=="*");
					{
						columnName=selectList[0];
					}
					columnNo=newTable.columnIndexMap.get(columnName);
				}
				else
				{
					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
					Pair colDescTypePair;
					colDescTypePair = new Pair(selectList[i].trim(),"DECIMAL");
					GroupByTable.columnDescription.add(colDescTypePair);
					if(columnName.equals("*"))
					{				
						columnName=selectList[0];
					}
					columnNo=newTable.columnIndexMap.get(columnName.trim());
				}

				HashMap<String,Double> CountMap=getGroupBy(newTable,check, columnName, columnNo,"COUNT");
				@SuppressWarnings("rawtypes")
				Iterator countIterator = CountMap.keySet().iterator();  

				//Iterating over the HashMap
				fr=new FileReader(new File(GroupByTable.tableDatFilePath));
				br=new BufferedReader(fr);

				line=br.readLine();
				arrTuple.clear();
				if(line!=null)
				{
					while (countIterator.hasNext()&&line!=null) 
					{  
						String key = countIterator.next().toString();
						
						Integer v = CountMap.get(key).intValue();
						String value = v.toString();

						arrTuple.add(line+"|"+value);
						line=br.readLine();
					}
					br.close();
					File filePointer = new File(GroupByTable.tableDatFilePath);

					if(filePointer.delete())
					{}
					fwr = new FileWriter(filePointer);
					bwr = new BufferedWriter(fwr);
					for(Object s:arrTuple)
					{
						bwr.write(s+"\n");
					}
					bwr.close();

				}
				else
				{
					bwr = new BufferedWriter(fwr);
					while (countIterator.hasNext()) 
					{  
						String key = countIterator.next().toString(); 
						Integer v = CountMap.get(key).intValue();
						String value = v.toString();

						bwr.write(key+"|"+value);
						bwr.write("\n");

					}
					bwr.close();
				}

			}

			if(selectList[i].contains("AVG")||selectList[i].contains("avg"))
			{
				if(selectList[i].contains("AS"))
				{
					str=selectList[i].substring(selectList[i].lastIndexOf(" ")+1,selectList[i].length());
					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
					columnNo=newTable.columnIndexMap.get(columnName);
					Pair colDescTypePair;
					colDescTypePair = new Pair(str.trim(),"DECIMAL");
					GroupByTable.columnDescription.add(colDescTypePair);
				}
				else
				{

					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].lastIndexOf(")"));
					columnNo=newTable.columnIndexMap.get(columnName);
					Pair colDescTypePair;
					colDescTypePair = new Pair(selectList[i].trim(),"DECIMAL");
					GroupByTable.columnDescription.add(colDescTypePair);
				}

				HashMap<String, Double> SumMap=getGroupBy(newTable, check, columnName, columnNo,"SUM");
				HashMap<String, Double> countMap=getGroupBy(newTable, check, columnName, columnNo,"COUNT");

				HashMap<String,Double> avgMap=new HashMap<String,Double>();

				@SuppressWarnings("rawtypes")
				Iterator AvgIterator = SumMap.keySet().iterator();  

				while (AvgIterator.hasNext()) 
				{  
					String key = AvgIterator.next().toString();  
					double average=SumMap.get(key)/countMap.get(key);
					avgMap.put(key, average);

				}
				@SuppressWarnings("rawtypes")

				Iterator Averageiterator = avgMap.keySet().iterator();  
				fr=new FileReader(new File(GroupByTable.tableDatFilePath));
				br=new BufferedReader(fr);						
				line=br.readLine();

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

						double value=avgMap.get(str1);
						//float value=avgMap.get(superKey);


						arrTuple.add(line+"|"+value);
						line=br.readLine();
					}

					File filePointer = new File(GroupByTable.tableDatFilePath);

					if(filePointer.delete()){}
					fwr = new FileWriter(filePointer);
					bwr = new BufferedWriter(fwr);
					for(Object s:arrTuple)
					{
						bwr.write(s+"\n");
					}
					bwr.close();
				}

				else if(line==null)
				{				

					bwr = new BufferedWriter(fwr);
					String superKey=null;
					while (Averageiterator.hasNext()) 
					{  
						String key = Averageiterator.next().toString();  

						double value=avgMap.get(key);

						bwr.write(key+"|"+value);
						bwr.write("\n");
					}	
					bwr.close();

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
		//System.out.println(GroupByTable.columnIndexMap);

		Table table = ProjectTableOperation.projectTable(GroupByTable,arrnew);//.printTuples();
		
		//System.out.println(table.columnIndexMap);
		if(orderByList != null){
			table = OrderByOperation.orderBy(table, orderByList, orderByList.size());
			
		}
		
		else{
		table.printTuples();
		}
		}

		return GroupByTable;
	}
}




/*//Aggregate Function to count Rows ie. Count(*) aggregate function
	//Returns a table containing the column names(Group BY ones) and the resultant aggregate Function

	public Table CountRows(Table newTable,String[] selectList,String groupBy,String orderBy) throws IOException
	{
		Integer count=0;
		String[] check=groupBy.split(",");
		Table GroupByTable=null;
		FileWriter fwr=null;
		GroupByTable = new Table("whereResultTable",newTable.dataDirectoryPath,newTable.dataDirectoryPath+System.getProperty("file.separator").toString()+newTable.tableName+"GroupBy.dat",newTable.noOfColumns);
		GroupByTable.columnDescription = newTable.columnDescription;
		GroupByTable.columnIndexMap = newTable.columnIndexMap;
		fwr = new FileWriter(new File(GroupByTable.tableDatFilePath));
		BufferedWriter bwr = new BufferedWriter(fwr);

		if(check[0].contains("NO"))
		{ 
			String canString=newTable.returnTuple();
			while(canString!=null)

			{	
				count++;

				canString=newTable.returnTuple();
			}
			System.out.println("Count::"+count);
			bwr.write("COUNT"+"|"+count.toString());
			bwr.close();
			return GroupByTable;
		}

		String concat=RetColumnName(newTable,selectList, groupBy);
		String[] aggregateFunc=concat.split(",");
		String aggString=aggregateFunc[0];
		String  columnName=aggregateFunc[1];

		int columnNo=newTable.columnIndexMap.get(columnName);
		HashMap<String, Integer> map=getGroupBy(newTable,groupBy, columnName, columnNo,aggString);

		@SuppressWarnings("rawtypes")
		Iterator iterator = map.keySet().iterator();  
		GroupByTable = new Table("whereResultTable",newTable.dataDirectoryPath,newTable.dataDirectoryPath+System.getProperty("file.separator").toString()+newTable.tableName+"GroupBy.dat",newTable.noOfColumns);
		GroupByTable.columnDescription = newTable.columnDescription;
		GroupByTable.columnIndexMap = newTable.columnIndexMap;
		fwr = new FileWriter(new File(GroupByTable.tableDatFilePath));
		BufferedWriter bwrNew=new BufferedWriter(fwr);
		//Iterating over the HashMAp
		while (iterator.hasNext()) 
		{  
			String key = iterator.next().toString();  
			String value = map.get(key).toString();  
			System.out.println(key + " " + value);  
			bwrNew.write(key+"|"+value);
			bwrNew.write("\n");
			bwrNew.flush();
		}
		bwr.close();




		return GroupByTable;
	}

	//Aggregate Function to calculate Mean ie. AVG aggregate function
	//Returns a table containing the column names(Group BY ones) and the resultant aggregate Function


	@SuppressWarnings("resource")
	public Table calMean(Table newTable,String[] selectList,String groupBy,String orderBy) throws IOException
	{
		int sum=0;
		boolean flag=false;
		int count=0;
		Float avg=(float) 0;
		String[] parts = null;
		Table groupByTable=null;
		String[] check=groupBy.split(",");
		String[] ByOrder=orderBy.split(" ");
		if(ByOrder.length==1)
		{
			flag=true;
		}

		if(check[0].contains("NO"))
		{
			Table SumTable=getSum(newTable, selectList, groupBy);
			FileReader fr=new FileReader(SumTable.tableDatFilePath);
			BufferedReader br=new BufferedReader(fr);
			String line=null;
			line=br.readLine();
			System.out.println(line);
			parts=line.split("\\|");
			sum=Integer.parseInt(parts[0]);

			//Get the count total to calculate average
			Table countTable=CountRows(newTable, selectList, groupBy,orderBy);
			fr=new FileReader(countTable.tableDatFilePath);
			br=new BufferedReader(fr);
			line=null;
			line=br.readLine();
			parts=line.split("\\|");
			count=Integer.parseInt(parts[1]);

			br.close();

			avg=((float)sum/(float)count);

			groupByTable = new Table("whereResultTable",newTable.dataDirectoryPath,newTable.dataDirectoryPath+System.getProperty("file.separator").toString()+newTable.tableName+"GroupBy.dat",newTable.noOfColumns);
			groupByTable.columnDescription = newTable.columnDescription;
			groupByTable.columnIndexMap = newTable.columnIndexMap;
			FileWriter fwr = new FileWriter(new File(groupByTable.tableDatFilePath));
			BufferedWriter bwr= new BufferedWriter(fwr);
			bwr.write("AVG"+"|"+avg.toString());
			System.out.println("Average::"+avg);
			bwr.close();
			return groupByTable;
		}


		String concat=RetColumnName(newTable, selectList, groupBy);
		String[] aggregateFunc=concat.split(",");
		String  columnName=aggregateFunc[1];
		int columnNo=newTable.columnIndexMap.get(columnName);
		HashMap<String, Integer> SumMap=getGroupBy(newTable, groupBy, columnName, columnNo,"SUM");
		HashMap<String, Integer> countMap=getGroupBy(newTable, groupBy, columnName, columnNo,"COUNT");

		HashMap<String,Float> avgMap=new HashMap<String,Float>();


		groupByTable = new Table("whereResultTable",newTable.dataDirectoryPath,newTable.dataDirectoryPath+System.getProperty("file.separator").toString()+newTable.tableName+"GroupBy.dat",newTable.noOfColumns);
		groupByTable.columnDescription = newTable.columnDescription;
		groupByTable.columnIndexMap = newTable.columnIndexMap;
		FileWriter fwr = new FileWriter(new File(groupByTable.tableDatFilePath));
		BufferedWriter bwrNew=new BufferedWriter(fwr);
		@SuppressWarnings("rawtypes")
		Iterator iterator = SumMap.keySet().iterator();  

		while (iterator.hasNext()) 
		{  
			String key = iterator.next().toString();  
			float average=((float)SumMap.get(key)/(float)countMap.get(key));
			avgMap.put(key, average);

		}

		@SuppressWarnings("rawtypes")
		Iterator Averageiterator = avgMap.keySet().iterator();  

		while (Averageiterator.hasNext()) 
		{  
			String key = Averageiterator.next().toString();  
			float value=avgMap.get(key);

			System.out.println(key +" "+ value);
			bwrNew.write(key+value);
			bwrNew.write("\n");

		}
		bwrNew.close();




		return groupByTable;

	}
	//Aggregate Function to calculate Minimum ie. MIN aggregate function
	//Returns a table containing the column names(Group BY ones) and the resultant aggregate Function


	public Table calMin(Table newTable,String[] selectList,String groupBy,String orderBy) throws IOException
	{
		int columnNo=0;
		Integer min = 0;
		String[] check=groupBy.split(",");
		HashMap<String, Integer> map;
		Table GroupByTable=null;
		FileWriter fwr=null;
		BufferedWriter bwr=null;
		int count=0;



		GroupByTable = new Table("MinResultTable",newTable.dataDirectoryPath,newTable.dataDirectoryPath+System.getProperty("file.separator").toString()+newTable.tableName+"GroupBy.dat",newTable.noOfColumns);
		GroupByTable.columnDescription = newTable.columnDescription;
		GroupByTable.columnIndexMap = newTable.columnIndexMap;	

		fwr = new FileWriter(new File(GroupByTable.tableDatFilePath));
		bwr = new BufferedWriter(fwr);	
		String columnName;
		//Check if there is n a group BY clause or not
		if(check[0].contains("NO"))
		{
			for(int i=0;i<selectList.length;i++)
			{
				columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].indexOf(")"));
				columnNo=newTable.columnIndexMap.get(columnName);
				String canString=newTable.returnTuple();
				String[] tupleList=canString.split("\\|");
				min=Integer.parseInt(tupleList[columnNo]);

				while(canString!=null)
				{			
					tupleList=canString.split("\\|");

					if(min>=Integer.parseInt(tupleList[columnNo]))
					{
						min=Integer.parseInt(tupleList[columnNo]);

					}
					canString=newTable.returnTuple();
				}
				bwr.write(min.toString()+"|");
				bwr.flush();
			}
			return GroupByTable;

		}

		else
		{
			for(int i=0;i<selectList.length;i++)
			{
				if(selectList[i].contains("MIN"))
				{
					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].indexOf(")"));
					columnNo=newTable.columnIndexMap.get(columnName);

					//Storing the aggregate function
					String aggString=selectList[i].substring(0,selectList[i].indexOf("("));

					map=getGroupBy(newTable, groupBy, columnName, columnNo, aggString);
					System.out.println(map);

					Iterator iterator = map.keySet().iterator();  

					if(count==0)
					{
						count++;
						GroupByTable = new Table("MinResultTable",newTable.dataDirectoryPath,newTable.dataDirectoryPath+System.getProperty("file.separator").toString()+newTable.tableName+"GroupBy.dat",newTable.noOfColumns);
						GroupByTable.columnDescription = newTable.columnDescription;
						GroupByTable.columnIndexMap = newTable.columnIndexMap;
						System.out.println(GroupByTable.tableDatFilePath);
						fwr = new FileWriter(new File(GroupByTable.tableDatFilePath));

						BufferedWriter bwrNew = new BufferedWriter(fwr);

						while (iterator.hasNext())
						{  
							String key = iterator.next().toString();  
							String value = map.get(key).toString();  

							System.out.println(key + " " + value);  
							bwrNew.write(key+value);
							bwrNew.write("\n");
							bwrNew.flush();
						} 

					}
					else 
					{
						FileReader fr=new FileReader(GroupByTable.tableDatFilePath);
						BufferedReader br=new BufferedReader(fr);
						String line=null;
						line=br.readLine();
						fwr = new FileWriter(new File(GroupByTable.tableDatFilePath));

						BufferedWriter bwrNew = new BufferedWriter(fwr);
						while(line!=null && iterator.hasNext())
						{

							String key = iterator.next().toString();  
							String value = map.get(key).toString(); 
							bwrNew.write(line+"|"+value);
							bwrNew.write("\n");
							bwrNew.flush();
							line=br.readLine();	
						}
					}
				}	
			}
		}


		return GroupByTable;
	}




	//Aggregate Function to calculate Maximum ie. MAX aggregate function
	//Returns a table containing the column names(Group BY ones) and the resultant aggregate Function

	public Table calMax(Table newTable,String[] selectList,String groupBy,String orderBy) throws IOException
	{
		int columnNo=0;
		Integer min = 0;
		boolean flag=false;
		String[] check=groupBy.split(",");
		HashMap<String, Integer> map;
		Table GroupByTable=null;
		FileWriter fwr=null;
		BufferedWriter bwr=null;
		String[] ByOrder=null;
		boolean flag1=false;
		int count=0;



		GroupByTable = new Table("MAXResultTable",newTable.dataDirectoryPath,newTable.dataDirectoryPath+System.getProperty("file.separator").toString()+newTable.tableName+"GroupBy.dat",newTable.noOfColumns);
		GroupByTable.columnDescription = newTable.columnDescription;
		GroupByTable.columnIndexMap = newTable.columnIndexMap;	

		fwr = new FileWriter(new File(GroupByTable.tableDatFilePath));
		bwr = new BufferedWriter(fwr);	
		String columnName;
		//Check if there is n a group BY clause or not
		if(check[0].contains("NO"))
		{
			for(int i=0;i<selectList.length;i++)
			{
				columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].indexOf(")"));
				columnNo=newTable.columnIndexMap.get(columnName);
				String canString=newTable.returnTuple();
				String[] tupleList=canString.split("\\|");
				min=Integer.parseInt(tupleList[columnNo]);

				while(canString!=null)
				{			
					tupleList=canString.split("\\|");

					if(min<=Integer.parseInt(tupleList[columnNo]))
					{
						min=Integer.parseInt(tupleList[columnNo]);

					}
					canString=newTable.returnTuple();
				}
				bwr.write(min.toString()+"|");
				bwr.flush();
			}
			return GroupByTable;

		}

		else
		{
			for(int i=0;i<selectList.length;i++)
			{
				if(selectList[i].contains("MAX"))
				{
					columnName=selectList[i].substring(selectList[i].indexOf("(")+1,selectList[i].indexOf(")"));
					columnNo=newTable.columnIndexMap.get(columnName);

					//Storing the aggregate function
					String aggString=selectList[i].substring(0,selectList[i].indexOf("("));

					map=getGroupBy(newTable, groupBy, columnName, columnNo, aggString);
					System.out.println(map);

					Iterator iterator = map.keySet().iterator();  

					if(count==0)
					{
						count++;
						GroupByTable = new Table("MinResultTable",newTable.dataDirectoryPath,newTable.dataDirectoryPath+System.getProperty("file.separator").toString()+newTable.tableName+"GroupBy.dat",newTable.noOfColumns);
						GroupByTable.columnDescription = newTable.columnDescription;
						GroupByTable.columnIndexMap = newTable.columnIndexMap;
						System.out.println(GroupByTable.tableDatFilePath);
						fwr = new FileWriter(new File(GroupByTable.tableDatFilePath));

						BufferedWriter bwrNew = new BufferedWriter(fwr);

						while (iterator.hasNext())
						{  
							String key = iterator.next().toString();  
							String value = map.get(key).toString();  

							System.out.println(key + " " + value);  
							bwrNew.write(key+"|"+value);
							bwrNew.write("\n");
							bwrNew.flush();
						} 

					}
					else 
					{
						FileReader fr=new FileReader(GroupByTable.tableDatFilePath);
						BufferedReader br=new BufferedReader(fr);
						String line=null;
						line=br.readLine();
						fwr = new FileWriter(new File(GroupByTable.tableDatFilePath));

						BufferedWriter bwrNew = new BufferedWriter(fwr);
						while(line!=null && iterator.hasNext())
						{

							String key = iterator.next().toString();  
							String value = map.get(key).toString(); 
							bwrNew.write(line+"|"+value);
							bwrNew.write("\n");
							bwrNew.flush();
							line=br.readLine();	
						}
					}
				}

			}
		}
		return GroupByTable;
	}
}




 */
