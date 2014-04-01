package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
import java.util.TreeSet;


import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;

/* This class is used for performing the selection operation on a given table */
public class SelectionOperation {

		
	// this method will apply selection operations on tableToAppySelectionOn 
	public static Table selectionOnTable(Expression expression,Table tableToAppySelectionOn) throws IOException,ParseException{
		
		//System.out.println(tableToAppySelectionOn);
		// this Table contains the resultant table after applying Selection Operation
		Table resultantTable = new Table("whereResultTable",tableToAppySelectionOn.dataDirectoryPath,tableToAppySelectionOn.dataDirectoryPath+System.getProperty("file.separator").toString()+tableToAppySelectionOn.tableName+"Where.dat",tableToAppySelectionOn.noOfColumns);
		
		resultantTable.columnDescription = tableToAppySelectionOn.columnDescription;
		resultantTable.columnIndexMap = tableToAppySelectionOn.columnIndexMap;
					
		// following conditions are to evaluate the expression
		if(expression instanceof EqualsTo || expression instanceof GreaterThanEquals || expression instanceof GreaterThan ||
				expression instanceof MinorThanEquals || expression instanceof MinorThan){
			//System.out.println(" IN comparisons");
			
			ArrayList<Integer> listOfIndices = expressionEvaluator(expression,tableToAppySelectionOn);

			populateTable(resultantTable, tableToAppySelectionOn, listOfIndices);
			
		}
		
		
		else if(expression instanceof InExpression){
			//System.out.println(((InExpression)expression).getStringExpression());
		}
		else if(expression instanceof LikeExpression){
			
		}
		else if(expression instanceof AndExpression){
			//System.out.println("in first AND");
			AndExpression mte = (AndExpression)expression;
			Expression leftVal = ((Expression)mte.getLeftExpression());
			Expression rightVal = ((Expression)mte.getRightExpression());
						

			ArrayList<Integer> leftArr = expressionEvaluator(leftVal, tableToAppySelectionOn);
			ArrayList<Integer> rightArr = expressionEvaluator(rightVal, tableToAppySelectionOn);
			
						
			ArrayList<Integer> listOfIndices = new ArrayList<Integer>();
			
			
			for(int i:leftArr){
				if(rightArr.contains(i)){
					listOfIndices.add(i);
				}
			}
			
			populateTable(resultantTable, tableToAppySelectionOn, listOfIndices);
			
			
		}
		else if(expression instanceof OrExpression){
			//System.out.println("in first OR");
			OrExpression mte = (OrExpression)expression;
			Expression leftVal = ((Expression)mte.getLeftExpression());
			Expression rightVal = ((Expression)mte.getRightExpression());
						
			//System.out.println(leftVal.toString());
			//System.out.println(rightVal.toString());
			
			ArrayList<Integer> leftArr = expressionEvaluator(leftVal, tableToAppySelectionOn);
			ArrayList<Integer> rightArr = expressionEvaluator(rightVal, tableToAppySelectionOn);
			
			
			TreeSet<Integer> set = new TreeSet<Integer>();
			for(int i:leftArr){
				set.add(i);
			}
			for(int i:rightArr){
				set.add(i);
			}
			
			ArrayList<Integer> listOfIndices = new ArrayList<Integer>();
			
			for(int i:set){
				listOfIndices.add(i);
			}
			
			populateTable(resultantTable, tableToAppySelectionOn, listOfIndices);
			
			
			
		}
		else if(expression instanceof Between){
			
		}
		else if(expression instanceof NotEqualsTo){
			
		}
		else if(expression instanceof NullValue){
			
		}
		else if(expression instanceof IsNullExpression){
			
		}
		
		/*
		 * And other where clause expressions
		 * 
		 * 
		*/
		
		return resultantTable;
	}
	
	// this method is to evaluate Where Expression
	// it seperates the comparison, logical and algebric expression in seperate blocks
	private static ArrayList<Integer> expressionEvaluator(Expression expression,Table tableToAppySelectionOn) throws IOException,ParseException{
			
		//System.out.println("in exp eval");
		
			ArrayList<Integer> listOfIndices = new ArrayList<Integer>();
		
			// this Table contains the resultant table after applying Selection Operation
				Table resultantTable = new Table("resultTable", tableToAppySelectionOn);
				
								
				// following conditions are to evaluate the EQUAL expression
				if(expression instanceof EqualsTo){
					//System.out.println("in equals");
					EqualsTo et = (EqualsTo)expression;
					
					
					Expression leftExpression = ((Expression)et.getLeftExpression());
					Expression rightExpression = ((Expression)et.getRightExpression());
					
					if(leftExpression instanceof BinaryExpression || rightExpression instanceof BinaryExpression){
						listOfIndices = alegbricExpressionEvaluator(et, tableToAppySelectionOn);
					
					}
					else{
						//System.out.println("in else");
						String leftVal = leftExpression.toString();
						String rightVal = rightExpression.toString();
					
					String type = tableToAppySelectionOn.columnDescription.get(tableToAppySelectionOn.columnIndexMap.get(leftVal)).columnTypeName;
					//System.out.println(type);
					String tuple = null;
					int tupleNo = 0;
					//System.out.println(type);
					while((tuple = tableToAppySelectionOn.returnTuple()) != null){
						tupleNo++;
						
						String array[] = tuple.split("\\|");
						
						int index = tableToAppySelectionOn.columnIndexMap.get(leftVal);
						String []rightArray = null;
						int rightIndex = 0;
						
						if(tableToAppySelectionOn.columnIndexMap.containsKey(rightVal)){
							rightIndex = tableToAppySelectionOn.columnIndexMap.get(rightVal);
							rightArray = array;
							//System.out.println("in contains");
							//System.out.println(rightArray[rightIndex]);
						}
						else{
							//System.out.println("in contains else");		
							rightArray = new String[1];
							rightArray[0] = rightVal;
							//System.out.println(rightArray[rightIndex]);
						}
						
						if(type.equalsIgnoreCase("string") || type.equalsIgnoreCase("char")){
							//System.out.println("in string");
							if(tableToAppySelectionOn.columnIndexMap.containsKey(rightVal)){
								if(array[index].equals(rightArray[rightIndex])){
									listOfIndices.add(tupleNo);
								
							}
							}else{
								if(array[index].equals(rightArray[rightIndex].substring(1, rightArray[rightIndex].length()-1))){
									listOfIndices.add(tupleNo);
								}
							}
						}
						else if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("decimal")){
							//System.out.println("--"+rightArray[rightIndex]+"---");
							//System.out.println(array[index]);
							//int a = Integer.parseInt(array[index]);
							//int b = Integer.parseInt(rightArray[rightIndex]);
							//System.out.println("testing");
							if(Double.parseDouble(array[index])==Double.parseDouble(rightArray[rightIndex])){
								listOfIndices.add(tupleNo);
							}
							
						
						}
						else if(type.equalsIgnoreCase("date")){
							String leftDate[] = array[index].split("-");
							String rightDate[] = rightArray[rightIndex].substring(6, rightArray[rightIndex].length()-2).split("-");	
							
							if(Integer.parseInt(leftDate[0])==Integer.parseInt(rightDate[0]) && Integer.parseInt(leftDate[1])==Integer.parseInt(rightDate[1]) && Integer.parseInt(leftDate[2])==Integer.parseInt(rightDate[2])){
								listOfIndices.add(tupleNo);
							}
							
						}
						
					}
					
					}
				}
				// evaluating >= expression
				else if(expression instanceof GreaterThanEquals){
					//System.out.println("in greater than");
					GreaterThanEquals gte = (GreaterThanEquals)expression;
					
					Expression leftExpression = ((Expression)gte.getLeftExpression());
					Expression rightExpression = ((Expression)gte.getRightExpression());
					
					if(leftExpression instanceof BinaryExpression || rightExpression instanceof BinaryExpression){
						listOfIndices = alegbricExpressionEvaluator(gte, tableToAppySelectionOn);
					
					}
					else{
					
							String leftVal = leftExpression.toString();
							String rightVal = rightExpression.toString();
							
							String type = tableToAppySelectionOn.columnDescription.get(tableToAppySelectionOn.columnIndexMap.get(leftVal)).columnTypeName;
							
							String tuple = null;
							int tupleNo = 0;
							
							
							while((tuple = tableToAppySelectionOn.returnTuple()) != null){
								tupleNo++;
						
								String array[] = tuple.split("\\|");
								int index = tableToAppySelectionOn.columnIndexMap.get(leftVal);
								
								
								String []rightArray = null;
								int rightIndex = 0;
								
								if(tableToAppySelectionOn.columnIndexMap.containsKey(rightVal)){
									rightIndex = tableToAppySelectionOn.columnIndexMap.get(rightVal);
									rightArray = array;
									//System.out.println(rightArray[rightIndex]);
								}
								else{
									rightArray = new String[1];
									rightArray[0] = rightVal;//.substring(0, rightVal.length());
									//System.out.println(rightArray[rightIndex]);
								}
													
								if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("decimal")){
									
									if(Double.parseDouble(array[index])>=Double.parseDouble(rightArray[rightIndex])){
										listOfIndices.add(tupleNo);
									}
								
								
								}
								else if(type.equalsIgnoreCase("date")){
									
						//			System.out.println("aaa"+rightVal);
									String leftDate[] = array[index].split("-");
									String rightDate[] = rightArray[rightIndex].substring(6, rightArray[rightIndex].length()-2).split("-");	
																
									if(Integer.parseInt(leftDate[0])<Integer.parseInt(rightDate[0])){
										
									}
									else if(Integer.parseInt(leftDate[0])==Integer.parseInt(rightDate[0])){
										if(Integer.parseInt(leftDate[1])<Integer.parseInt(rightDate[1])){}
										else if(Integer.parseInt(leftDate[1])==Integer.parseInt(rightDate[1])){
											if(Integer.parseInt(leftDate[2])<Integer.parseInt(rightDate[2])){}
											else{
												listOfIndices.add(tupleNo);
											}
										}
										else{
											listOfIndices.add(tupleNo);
										}
									}else{
										listOfIndices.add(tupleNo);
									}
								}
								
							}
					
					}
					
						
				}
				
				// evaluating > expression
				else if(expression instanceof GreaterThan){
					//System.out.println("in greater");
					GreaterThan gt = (GreaterThan)expression;
					
					
					
					Expression leftExpression = ((Expression)gt.getLeftExpression());
					Expression rightExpression = ((Expression)gt.getRightExpression());
					
					if(leftExpression instanceof BinaryExpression || rightExpression instanceof BinaryExpression){
						listOfIndices = alegbricExpressionEvaluator(gt, tableToAppySelectionOn);
					
					}
					else{
					
						
						String leftVal = leftExpression.toString();
						String rightVal = rightExpression.toString();
														
					String type = tableToAppySelectionOn.columnDescription.get(tableToAppySelectionOn.columnIndexMap.get(leftVal)).columnTypeName;
					
					String tuple = null;
					int tupleNo = 0;
					
					while((tuple = tableToAppySelectionOn.returnTuple()) != null){
						tupleNo++;
				
						String array[] = tuple.split("\\|");
						int index = tableToAppySelectionOn.columnIndexMap.get(leftVal);
								
						
						String []rightArray = null;
						int rightIndex = 0;
						
						if(tableToAppySelectionOn.columnIndexMap.containsKey(rightVal)){
							rightIndex = tableToAppySelectionOn.columnIndexMap.get(rightVal);
							rightArray = array;
							//System.out.println(rightArray[rightIndex]);
						}
						else{
							rightArray = new String[1];
							rightArray[0] = rightVal;//.substring(0, rightVal.length());
							//System.out.println(rightArray[rightIndex]);
						}
						//System.out.println(array[index]);
						//System.out.println(rightArray[rightIndex]);
						//System.out.println(rightVal);
						if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("decimal")){
							
							if(Double.parseDouble(array[index])>Double.parseDouble(rightArray[rightIndex])){
								listOfIndices.add(tupleNo);
							}
							
						}
						else if(type.equalsIgnoreCase("date")){
						//	System.out.println(rightVal);
							//DateFormat df = DateFormat.getDateInstance(DateFormat., Locale.US);
							String leftDate[] = array[index].split("-");
							String rightDate[] = rightArray[rightIndex].substring(6, rightArray[rightIndex].length()-2).split("-");	
														
							if(Integer.parseInt(leftDate[0])<Integer.parseInt(rightDate[0])){
								
							}
							else if(Integer.parseInt(leftDate[0])==Integer.parseInt(rightDate[0])){
								if(Integer.parseInt(leftDate[1])<Integer.parseInt(rightDate[1])){}
								else if(Integer.parseInt(leftDate[1])==Integer.parseInt(rightDate[1])){
									if(Integer.parseInt(leftDate[2])<=Integer.parseInt(rightDate[2])){}
									else{
										listOfIndices.add(tupleNo);
									}
								}
								else{
									listOfIndices.add(tupleNo);
								}
							}else{
								listOfIndices.add(tupleNo);
							}
							
						}
						
					}
					
					
					}
						
				}
				
				// evaluating < expression
				else if(expression instanceof MinorThan){
					MinorThan mt = (MinorThan)expression;
									
					//System.out.println("in minor");
					Expression leftExpression = ((Expression)mt.getLeftExpression());
					Expression rightExpression = ((Expression)mt.getRightExpression());
					
					if(leftExpression instanceof BinaryExpression || rightExpression instanceof BinaryExpression){
						listOfIndices = alegbricExpressionEvaluator(mt, tableToAppySelectionOn);
					
					}
					else{
					
						
						String leftVal = leftExpression.toString();
						String rightVal = rightExpression.toString();
						//System.out.println(leftVal);
						//System.out.println("else  "+rightVal);
					
					String type = tableToAppySelectionOn.columnDescription.get(tableToAppySelectionOn.columnIndexMap.get(leftVal)).columnTypeName;
					
					String tuple = null;
					int tupleNo = 0;
					//System.out.println("type is "+type);
					
					while((tuple = tableToAppySelectionOn.returnTuple()) != null){
						tupleNo++;
				
						String array[] = tuple.split("\\|");
						int index = tableToAppySelectionOn.columnIndexMap.get(leftVal);
						
						String []rightArray = null;
						int rightIndex = 0;
						
						if(tableToAppySelectionOn.columnIndexMap.containsKey(rightVal)){
							rightIndex = tableToAppySelectionOn.columnIndexMap.get(rightVal);
							rightArray = array;
							//System.out.println(rightArray[rightIndex]);
						}
						else{
							rightArray = new String[1];
							rightArray[0] = rightVal;//.substring(0, rightVal.length());
							//System.out.println(rightArray[rightIndex]);
						}
						
										
						if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("decimal")){
							
						//	System.out.println("in int");
							if(Double.parseDouble(array[index])<Double.parseDouble(rightArray[rightIndex])){
								listOfIndices.add(tupleNo);
							}
							
						
						
						}
						else if(type.equalsIgnoreCase("date")){
							//System.out.println("dfgdfg"+rightVal);
							
							String leftDate[] = array[index].split("-");
							String rightDate[] = rightArray[rightIndex].substring(6, rightArray[rightIndex].length()-2).split("-");	
														
							/*for(String s:leftDate){
								System.out.print(s+" ");
							}
							for(String s:rightDate){
								System.out.print(s+" ");
							}*/
							
							if(Integer.parseInt(leftDate[0])>Integer.parseInt(rightDate[0])){
								
							}
							else if(Integer.parseInt(leftDate[0])==Integer.parseInt(rightDate[0])){
								if(Integer.parseInt(leftDate[1])>Integer.parseInt(rightDate[1])){}
								else if(Integer.parseInt(leftDate[1])==Integer.parseInt(rightDate[1])){
									if(Integer.parseInt(leftDate[2])>=Integer.parseInt(rightDate[2])){}
									else{
							//			System.out.println("date");
										listOfIndices.add(tupleNo);
									}
								}
								else{
						//			System.out.println("month");
									listOfIndices.add(tupleNo);
								}
							}else{
					//			System.out.println("year");
								listOfIndices.add(tupleNo);
							}
						}
						
					}
					
					}
					//System.out.println(listOfIndices.size());
				}
				
				// evaluating <= expression
				else if(expression instanceof MinorThanEquals){
					MinorThanEquals mte = (MinorThanEquals)expression;
										
					Expression leftExpression = ((Expression)mte.getLeftExpression());
					Expression rightExpression = ((Expression)mte.getRightExpression());
					
					if(leftExpression instanceof BinaryExpression || rightExpression instanceof BinaryExpression){
						listOfIndices = alegbricExpressionEvaluator(mte, tableToAppySelectionOn);
					
					}
					else{
					
						
						String leftVal = leftExpression.toString();
						String rightVal = rightExpression.toString();
						
					
					String type = tableToAppySelectionOn.columnDescription.get(tableToAppySelectionOn.columnIndexMap.get(leftVal)).columnTypeName;
					
					String tuple = null;
					int tupleNo = 0;
					
					
					while((tuple = tableToAppySelectionOn.returnTuple()) != null){
						tupleNo++;
				
						String array[] = tuple.split("\\|");
						int index = tableToAppySelectionOn.columnIndexMap.get(leftVal);
						
						
						String []rightArray = null;
						int rightIndex = 0;
						
						if(tableToAppySelectionOn.columnIndexMap.containsKey(rightVal)){
							rightIndex = tableToAppySelectionOn.columnIndexMap.get(rightVal);
							rightArray = array;
							//System.out.println(rightArray[rightIndex]);
						}
						else{
							rightArray = new String[1];
							rightArray[0] = rightVal;//.substring(0, rightVal.length());
							//System.out.println(rightArray[rightIndex]);
						}
						
						
						if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("decimal")){
							
							
							if(Double.parseDouble(array[index])<=Double.parseDouble(rightArray[rightIndex])){
								listOfIndices.add(tupleNo);
							}
							
						
						
						}
						else if(type.equalsIgnoreCase("date")){
							String leftDate[] = array[index].split("-");
							String rightDate[] = rightArray[rightIndex].substring(6, rightArray[rightIndex].length()-2).split("-");	
														
							if(Integer.parseInt(leftDate[0])>Integer.parseInt(rightDate[0])){
								
							}
							else if(Integer.parseInt(leftDate[0])==Integer.parseInt(rightDate[0])){
								if(Integer.parseInt(leftDate[1])>Integer.parseInt(rightDate[1])){}
								else if(Integer.parseInt(leftDate[1])==Integer.parseInt(rightDate[1])){
									if(Integer.parseInt(leftDate[2])>Integer.parseInt(rightDate[2])){}
									else{
										listOfIndices.add(tupleNo);
									}
								}
								else{
									listOfIndices.add(tupleNo);
								}
							}else{
								listOfIndices.add(tupleNo);
							}
						}
						
					}
					
					}
					
				}
				// evaluating AND expression
				else if(expression instanceof AndExpression){
					//System.out.println("in AND");
					AndExpression mte = (AndExpression)expression;
					Expression leftVal = ((Expression)mte.getLeftExpression());
					Expression rightVal = ((Expression)mte.getRightExpression());
					
					
					ArrayList<Integer> leftArr = expressionEvaluator(leftVal, tableToAppySelectionOn);
					ArrayList<Integer> rightArr = expressionEvaluator(rightVal, tableToAppySelectionOn);
					
					ArrayList<Integer> set = new ArrayList<Integer>();
					
					for(int i:leftArr){
						if(rightArr.contains(i)){
							set.add(i);
						}
					}
					listOfIndices = set;
					
					
				}
				// evaluating OR expression
				else if(expression instanceof OrExpression){
					//System.out.println("in OR");
					OrExpression mte = (OrExpression)expression;
					Expression leftVal = ((Expression)mte.getLeftExpression());
					Expression rightVal = ((Expression)mte.getRightExpression());
								
					ArrayList<Integer> leftArr = expressionEvaluator(leftVal, tableToAppySelectionOn);
					ArrayList<Integer> rightArr = expressionEvaluator(rightVal, tableToAppySelectionOn);
					
										
					TreeSet<Integer> set = new TreeSet<Integer>();
					for(int i:leftArr){
						set.add(i);
					}
					for(int i:rightArr){
						set.add(i);
					}
					
					for(int i:set){
						listOfIndices.add(i);
					}
				}
				
				
		
		return listOfIndices;
	}
	
	// this method evaluates the comparison expressions having algebric expressions
	private static ArrayList<Integer> alegbricExpressionEvaluator(Expression expression,Table tableToAppySelectionOn) throws IOException,ParseException{
		//System.out.println("in algebra");
		
		ArrayList<Integer> listOfIndices = new ArrayList<Integer>();
		String leftExp[];
		String rightExp[];
		
		
		if(expression instanceof GreaterThanEquals){
			String exps[]=expression.toString().split(">=");
				
				ArrayList<String> left = braceExp(exps[0]);
				ArrayList<String> right = braceExp(exps[1]);
				String []leftArr = new String[left.size()];
				String []rightArr = new String[right.size()];
				leftArr = left.toArray(leftArr);
				rightArr = right.toArray(rightArr);
				
				leftExp = convertToPos(leftArr);
				rightExp = convertToPos(rightArr);
				//leftExp = convertToPos(exps[0].split(" "));
				//rightExp = convertToPos(exps[1].split(" "));
				
				/*for(String s:leftExp){
					System.out.print(s+" ");
				}
				System.out.println("");
				for(String s:rightExp){
					System.out.print(s+" ");
				}
				System.out.println("");*/
				String tuple = null;
				int tupleNo = 0;
				
				while((tuple = tableToAppySelectionOn.returnTuple()) != null){
					tupleNo++;
					
					
					String array[] = tuple.split("\\|");
					String evalLeftStack[] = new String[leftExp.length]; 
					String evalRightStack[] = new String[rightExp.length];	 

					
					for (int j = 0; j < leftExp.length; j++) {
						if(tableToAppySelectionOn.columnIndexMap.containsKey(leftExp[j])){
							int index = tableToAppySelectionOn.columnIndexMap.get(leftExp[j]);
						
							evalLeftStack[j] = array[index];
						}
						else{
							evalLeftStack[j]=leftExp[j];
						}
						
					}
					
					for (int j = 0; j < rightExp.length; j++) {
						if(tableToAppySelectionOn.columnIndexMap.containsKey(rightExp[j])){
							int index = tableToAppySelectionOn.columnIndexMap.get(rightExp[j]);
							evalRightStack[j] = array[index];
						}
						else{
							evalRightStack[j] = rightExp[j];
						}
					}
					
					
						if(evaluate(evalLeftStack)>=evaluate(evalRightStack)){
							//System.out.println("true");
							listOfIndices.add(tupleNo);
						}
					
					
				}
				
		
		}
		else if (expression instanceof GreaterThan) {
			String exps[]=expression.toString().split(">");
		
			ArrayList<String> left = braceExp(exps[0]);
			ArrayList<String> right = braceExp(exps[1]);
			String []leftArr = new String[left.size()];
			String []rightArr = new String[right.size()];
			leftArr = left.toArray(leftArr);
			rightArr = right.toArray(rightArr);
			
			leftExp = convertToPos(leftArr);
			rightExp = convertToPos(rightArr);
		
			//leftExp = convertToPos(exps[0].split(" "));
			//rightExp = convertToPos(exps[1].split(" "));
			
			/*for(String s:rightExp){
				System.out.print(s+" ");
			}*/
			
			String evalLeftStack[] = new String[leftExp.length]; 
			String evalRightStack[] = new String[rightExp.length];	 
			
						
			String tuple = null;
			int tupleNo = 0;
			
			
			while((tuple = tableToAppySelectionOn.returnTuple()) != null){
				tupleNo++;
				
				
				
				String array[] = tuple.split("\\|");
				
				
							
				for (int j = 0; j < leftExp.length; j++) {
					if(tableToAppySelectionOn.columnIndexMap.containsKey(leftExp[j])){
						int index = tableToAppySelectionOn.columnIndexMap.get(leftExp[j]);
						evalLeftStack[j] = array[index];
					}
					else{
						evalLeftStack[j]=leftExp[j];
					}
					
				}
				for (int j = 0; j < rightExp.length; j++) {
					if(tableToAppySelectionOn.columnIndexMap.containsKey(rightExp[j])){
						int index = tableToAppySelectionOn.columnIndexMap.get(rightExp[j]);
						evalRightStack[j] = array[index];
					}
					else{
						evalRightStack[j] = rightExp[j];
					}
				}
			
				
				
				if(evaluate(evalLeftStack)>evaluate(evalRightStack)){
					
					listOfIndices.add(tupleNo);
				}
				
			}
			
			
			
			
		}
		else if(expression instanceof MinorThanEquals){
			String exps[]=expression.toString().split("<=");
			//evaluate(((GreaterThanEquals) expression).getLeftExpression());
			
			
			ArrayList<String> left = braceExp(exps[0]);
			ArrayList<String> right = braceExp(exps[1]);
			String []leftArr = new String[left.size()];
			String []rightArr = new String[right.size()];
			leftArr = left.toArray(leftArr);
			rightArr = right.toArray(rightArr);
			
			leftExp = convertToPos(leftArr);
			rightExp = convertToPos(rightArr);
		
			/*leftExp = convertToPos(exps[0].split(" "));
			rightExp = convertToPos(exps[1].split(" "));*/
			
			
			
			//String type = tableToAppySelectionOn.columnDescription.get(tableToAppySelectionOn.columnIndexMap.get(left)).columnTypeName;
			
			String tuple = null;
			int tupleNo = 0;
			
			
			while((tuple = tableToAppySelectionOn.returnTuple()) != null){
				tupleNo++;
				
				
				
				String array[] = tuple.split("\\|");
				String evalLeftStack[] = new String[leftExp.length]; 
				String evalRightStack[] = new String[rightExp.length];	 
				
				
				//if(tableToAppySelectionOn.columnIndexMap.containsKey(leftExp[]))
				
				for (int j = 0; j < leftExp.length; j++) {
					if(tableToAppySelectionOn.columnIndexMap.containsKey(leftExp[j])){
						int index = tableToAppySelectionOn.columnIndexMap.get(leftExp[j]);
						evalLeftStack[j] = array[index];
					}
					else{
						evalLeftStack[j]=leftExp[j];
					}
					
				}
				for (int j = 0; j < rightExp.length; j++) {
					if(tableToAppySelectionOn.columnIndexMap.containsKey(rightExp[j])){
						int index = tableToAppySelectionOn.columnIndexMap.get(rightExp[j]);
						evalRightStack[j] = array[index];
					}
					else{
						evalRightStack[j] = rightExp[j];
					}
				}
			
				//System.out.println(tupleNo);
				
				if(evaluate(evalLeftStack)<=evaluate(evalRightStack)){
					//System.out.println("true");
					listOfIndices.add(tupleNo);
				}
				
			}
		}
		
		else if(expression instanceof MinorThan){
			String exps[]=expression.toString().split("<");
			//evaluate(((GreaterThanEquals) expression).getLeftExpression());
			
			ArrayList<String> left = braceExp(exps[0]);
			ArrayList<String> right = braceExp(exps[1]);
			String []leftArr = new String[left.size()];
			String []rightArr = new String[right.size()];
			leftArr = left.toArray(leftArr);
			rightArr = right.toArray(rightArr);
			
			leftExp = convertToPos(leftArr);
			rightExp = convertToPos(rightArr);
			
			
			/*leftExp = convertToPos(exps[0].split(" "));
			rightExp = convertToPos(exps[1].split(" "));*/
			
			
			
			//String type = tableToAppySelectionOn.columnDescription.get(tableToAppySelectionOn.columnIndexMap.get(left)).columnTypeName;
			
			String tuple = null;
			int tupleNo = 0;
			
			
			while((tuple = tableToAppySelectionOn.returnTuple()) != null){
				tupleNo++;
				
				
				
				String array[] = tuple.split("\\|");
				String evalLeftStack[] = new String[leftExp.length]; 
				String evalRightStack[] = new String[rightExp.length];	 
				
				
				//if(tableToAppySelectionOn.columnIndexMap.containsKey(leftExp[]))
				
				for (int j = 0; j < leftExp.length; j++) {
					if(tableToAppySelectionOn.columnIndexMap.containsKey(leftExp[j])){
						int index = tableToAppySelectionOn.columnIndexMap.get(leftExp[j]);
						evalLeftStack[j] = array[index];
					}
					else{
						evalLeftStack[j]=leftExp[j];
					}
					
				}
				for (int j = 0; j < rightExp.length; j++) {
					if(tableToAppySelectionOn.columnIndexMap.containsKey(rightExp[j])){
						int index = tableToAppySelectionOn.columnIndexMap.get(rightExp[j]);
						evalRightStack[j] = array[index];
					}
					else{
						evalRightStack[j] = rightExp[j];
					}
				}
			
				//System.out.println(tupleNo);
				
				if(evaluate(evalLeftStack)<evaluate(evalRightStack)){
					//System.out.println("true");
					listOfIndices.add(tupleNo);
				}
				
			}
		}
		
		else if(expression instanceof EqualsTo){
			String exps[]=expression.toString().split("=");
			//evaluate(((GreaterThanEquals) expression).getLeftExpression());
			
			
			ArrayList<String> left = braceExp(exps[0]);
			ArrayList<String> right = braceExp(exps[1]);
			String []leftArr = new String[left.size()];
			String []rightArr = new String[right.size()];
			leftArr = left.toArray(leftArr);
			rightArr = right.toArray(rightArr);
			
			leftExp = convertToPos(leftArr);
			rightExp = convertToPos(rightArr);
		
			/*leftExp = convertToPos(exps[0].split(" "));
			rightExp = convertToPos(exps[1].split(" "));*/
			
			
			String tuple = null;
			int tupleNo = 0;
			
			
			while((tuple = tableToAppySelectionOn.returnTuple()) != null){
				tupleNo++;
				
				
				
				String array[] = tuple.split("\\|");
				String evalLeftStack[] = new String[leftExp.length]; 
				String evalRightStack[] = new String[rightExp.length];	 
				
				//if(tableToAppySelectionOn.columnIndexMap.containsKey(leftExp[]))
				
				for (int j = 0; j < leftExp.length; j++) {
					if(tableToAppySelectionOn.columnIndexMap.containsKey(leftExp[j])){
						int index = tableToAppySelectionOn.columnIndexMap.get(leftExp[j]);
						evalLeftStack[j] = array[index];
					}
					else{
						evalLeftStack[j]=leftExp[j];
					}
					
				}
				for (int j = 0; j < rightExp.length; j++) {
					if(tableToAppySelectionOn.columnIndexMap.containsKey(rightExp[j])){
						int index = tableToAppySelectionOn.columnIndexMap.get(rightExp[j]);
						evalRightStack[j] = array[index];
					}
					else{
						evalRightStack[j] = rightExp[j];
					}
				}
			
				//System.out.println(tupleNo);
				
				if(evaluate(evalLeftStack)==evaluate(evalRightStack)){
					//System.out.println("true");
					listOfIndices.add(tupleNo);
				}
				
			}
		}
		
		
		
		return listOfIndices;
	}


	// This method converts String expression to String[] 
	// to be used by Abhinav
	
	public static ArrayList<String> braceExp(String str) throws ParseException {
		
		
		CCJSqlParser parser = new CCJSqlParser(new StringReader(str));

		Expression exp = parser.SimpleExpression();
		//System.out.println(exp.toString());
		ArrayList<String> strArrList = new ArrayList<String>();
		if(exp instanceof Parenthesis){
			Expression binaryExp = ((Parenthesis)exp).getExpression();
			strArrList.add("(");
			if(binaryExp instanceof BinaryExpression){
				for(String s:braceExp(binaryExp.toString())){
					strArrList.add(s);
				}
			}
			strArrList.add(")");
	
		}
		else if(exp instanceof Addition){
			Expression leftExpression = ((Addition)exp).getLeftExpression();
			Expression rightExpression = ((Addition)exp).getRightExpression();
			ArrayList<String> leftExp = new ArrayList<String>();
			ArrayList<String> rightExp = new ArrayList<String>();
	
			if(leftExpression instanceof Parenthesis || leftExpression instanceof BinaryExpression){
				leftExp = braceExp(leftExpression.toString());
			}
			else{
				leftExp.add(leftExpression.toString());
			}
			rightExp.add("+");
			if(rightExpression instanceof Parenthesis || rightExpression instanceof BinaryExpression){
				for(String s:braceExp(rightExpression.toString())){
					rightExp.add(s);
				}
			}
			else{
				rightExp.add(rightExpression.toString());
			}
			for(String s:leftExp){
				strArrList.add(s);
			}
			for(String s:rightExp){
				strArrList.add(s);
			}
		
		}
		else if(exp instanceof Multiplication){
			Expression leftExpression = ((Multiplication)exp).getLeftExpression();
			Expression rightExpression = ((Multiplication)exp).getRightExpression();
			ArrayList<String> leftExp = new ArrayList<String>();
			ArrayList<String> rightExp = new ArrayList<String>();
			
			if(leftExpression instanceof Parenthesis || leftExpression instanceof BinaryExpression){
				leftExp = braceExp(leftExpression.toString());
			}
			else{
				leftExp.add(leftExpression.toString());
			}
			rightExp.add("*");
			if(rightExpression instanceof Parenthesis || rightExpression instanceof BinaryExpression){
				for(String s:braceExp(rightExpression.toString())){
					rightExp.add(s);
				}
			}
			else{
				rightExp.add(rightExpression.toString());
			}
			for(String s:leftExp){
				strArrList.add(s);
			}
			for(String s:rightExp){
				strArrList.add(s);
			}
		
		}
		else if(exp instanceof Subtraction){
			Expression leftExpression = ((Subtraction)exp).getLeftExpression();
			Expression rightExpression = ((Subtraction)exp).getRightExpression();
			ArrayList<String> leftExp = new ArrayList<String>();
			ArrayList<String> rightExp = new ArrayList<String>();
			if(leftExpression instanceof Parenthesis || leftExpression instanceof BinaryExpression){
				leftExp = braceExp(leftExpression.toString());
			}
			else{
				leftExp.add(leftExpression.toString());
			}
			rightExp.add("-");
			if(rightExpression instanceof Parenthesis || rightExpression instanceof BinaryExpression){
				for(String s:braceExp(rightExpression.toString())){
					rightExp.add(s);
				}
			}
			else{
				rightExp.add(rightExpression.toString());
			}
			for(String s:leftExp){
				strArrList.add(s);
			}
			for(String s:rightExp){
				strArrList.add(s);
			}
		
		}
		else if(exp instanceof Division){
			Expression leftExpression = ((Division)exp).getLeftExpression();
			Expression rightExpression = ((Division)exp).getRightExpression();
			ArrayList<String> leftExp = new ArrayList<String>();
			ArrayList<String> rightExp = new ArrayList<String>();
			if(leftExpression instanceof Parenthesis || leftExpression instanceof BinaryExpression){
				leftExp = braceExp(leftExpression.toString());
			}
			else{
				leftExp.add(leftExpression.toString());
			}
			rightExp.add("/");
			if(rightExpression instanceof Parenthesis || rightExpression instanceof BinaryExpression){
				for(String s:braceExp(rightExpression.toString())){
					rightExp.add(s);
				}
			}
			else{
				rightExp.add(rightExpression.toString());
			}
			for(String s:leftExp){
				strArrList.add(s);
			}
			for(String s:rightExp){
				strArrList.add(s);
			}
			
		}
		else{
			strArrList.add(exp.toString());
		}
		
		return strArrList;
		
	}
	
	
	
	// this method is to convert infix expression to postfix expression
	public static String[] convertToPos(String[] string) {
		
		
		HashMap<String,Integer> operator = new HashMap<String,Integer>();
		operator.put("*",2);
		operator.put("/",2);
		operator.put("+",1);
		operator.put("-",1);
		operator.put("(",3);
		operator.put(")",0);
		
		
		
		
		Stack<String> evaluationStack = new Stack<String>();
		Stack<String> operatorStack = new Stack<String>();
		
		for (int i = 0; i < string.length; i++) {
			if(!(operator.containsKey(string[i]))){
				
				evaluationStack.push(string[i]);
			}
			else{

						
					int precedence = operator.get(string[i]);
					if(!operatorStack.isEmpty()){
						if(operator.get(operatorStack.peek())>=precedence){
							//System.out.println(true+" "+string[i]);
							if(!(operatorStack.peek().equals("(") || operatorStack.peek().equals(")") || operatorStack.peek().equals("(")))
							evaluationStack.push(operatorStack.pop());
						
						}
					}
					
				
				operatorStack.push(string[i]);
			}
		}
		
		
		while(!operatorStack.isEmpty()){
			if(!((operatorStack.peek().equals(")") || operatorStack.peek().equals("(")))){
				evaluationStack.push(operatorStack.pop());
				
			}
			else{
				operatorStack.pop();
			}
			
		}
		String str[] =new String[evaluationStack.size()];
		
		int c = str.length-1;
		while(!evaluationStack.isEmpty()){
			str[c]=evaluationStack.pop();
			c--;
		}
		
		/*for(String s:str)
		 System.out.print(s+" ");*/
		
		return str;
	
	}

	// this method is to evaluate the postfix expression
	public static double evaluate(String arr[]) {
		HashMap<String,Integer> operators = new HashMap<String,Integer>();
		operators.put("*",1);
		operators.put("/",2);
		operators.put("+",3);
		operators.put("-",4);
		
		/*for(String s:arr){
			System.out.print(s+" ");
		}
		System.out.println("");*/
		Stack<String> evaluationStack = new Stack<String>();
		for(String s:arr){
			if(!operators.containsKey(s)){
				evaluationStack.add(s);
			}
			else{
				double val1= Double.parseDouble(evaluationStack.pop());
				double val2=  Double.parseDouble(evaluationStack.pop());
				double result = 0.0;
				if(operators.get(s)==1){
					result = val1 * val2;
				}
				else if(operators.get(s)==2){
					result = val2/val1;
				}
				else if(operators.get(s)==3){
					result = val1+val2;
				}
				else if (operators.get(s)==4) {
					result = val2-val1;
				}
				//System.out.println(result);
				evaluationStack.push(Double.toString(result));
			}
		}
		
		/*System.out.println(evaluationStack.size());
		System.out.println(evaluationStack.isEmpty());
		//System.out.println(evaluationStack.peek());
*/		return Double.parseDouble(evaluationStack.firstElement());
	}
	
	
	// this method populates the new table
	private static void populateTable(Table resultantTable,Table tableToAppySelectionOn, ArrayList<Integer> listOfIndices) throws IOException{
		
		// now write to the .dat file describing the Where clause
					FileWriter fwr = new FileWriter(new File(resultantTable.tableDatFilePath));
					BufferedWriter bwr = new BufferedWriter(fwr);
					
					String tuple;
					int tupleNo = 0;
					
					
					while((tuple=tableToAppySelectionOn.returnTuple()) != null){
						tupleNo++;
						if(listOfIndices.contains(tupleNo)){
							bwr.write(tuple);
							bwr.write("\n");
							bwr.flush();
						}
						
					}
		
	}
	
	
}