package com.cmpe281.spark;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Pair implements Serializable{
public Pair( String fileName) {
		// TODO Auto-generated constructor stub
	 
	 this.fileName = fileName;
	 this.lineNo = new ArrayList<Integer>();
	}
public Pair(String fileName, int count, List<Integer> lineNo){
	this.fileName = fileName;
	this.count = count;
	this.lineNo = lineNo;
}
public String getFileName() {
	return fileName;
}
public void setFileName(String fileName) {
	this.fileName = fileName;
	this.count = 1;
}
public int getCount(){
	return count;
}
public void setCount(int count){
	this.count = count;
}
public boolean equals(Object other){
	;
	 if (other == null || this.getClass() != other.getClass()) return false;
	 Pair p = (Pair)other;

	if(fileName.equals(p.fileName)){
		
		return true;
	}
	else
	return false;
	
}
public String toString(){
	//System.out.println("{ "+word + " " + " " + fileName + " } ");
	return "{ "+fileName + "," + count + ",<"+ lineNo + "> } ";
}

String fileName;
List<Integer> lineNo= new ArrayList<Integer>();;
public List<Integer> getLineNo() {
	return lineNo;
}
public void setLineNo(List<Integer> lineNo) {
	this.lineNo = lineNo;
}
int count=1;
public void addOne() {
	count = count + 1;
	
}
}
