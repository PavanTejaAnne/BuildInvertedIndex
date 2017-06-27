package com.cmpe281.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Sample Spark application that counts the words in a text file
 */
public class RealTimeSpark
{

    public static void invertSpark( String[] args )
    {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkApp");

        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load the input data, which is a text file read from the command line
        JavaPairRDD<String,String> input = sc.wholeTextFiles( args[0] );
   
        JavaPairRDD<String,String> middle = input.flatMapValues(new Function<String,Iterable<String>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String arg0) throws Exception {
				String[] lines = arg0.split(System.getProperty("line.separator"));
				int len = lines.length;
				for(int i=0;i<len;i++){
					lines[i]=lines[i]+"#"+(i+1);
					
				}
				
				return Arrays.asList(lines);
			}
        	
        
        });
        
        JavaPairRDD<String, String> output = middle.flatMapValues(
        	    new Function<String,Iterable<String>>() {
        	       
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<String> call(String arg0) throws Exception {
						String[] split = arg0.split("\\W");
						int len = split.length;
						String actSplit[] = new String[len-1];
						
						for(int i=0;i<len-1;i++){
							actSplit[i] = split[i] + "#" + split[len-1]; 
						}
						return Arrays.asList(actSplit);
					}

					
        	});
        JavaPairRDD<String,String> swappedBusinessStars = output.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
            public Tuple2<String, String> call(Tuple2<String, String> stringIntegerTuple2) throws Exception {
            	String[] split = stringIntegerTuple2._2.split("#");
            	String firstTuple = split[0];
            	String secondTuple = stringIntegerTuple2._1 + "#" + split[1];
            	return new Tuple2(firstTuple,secondTuple);
            } 
        });
        
        
        List<String> finalOutput = swappedBusinessStars.lookup(args[1]);
        JavaRDD<String> rddOutput = (JavaRDD) sc.parallelize(finalOutput);
        System.out.println(finalOutput);
       
       JavaPairRDD<String,String> rddPair = rddOutput.mapToPair(new PairFunction<String,String,String>(){

	

		@Override
		public Tuple2<String, String> call(String arg0) throws Exception {
			String split[] = arg0.split("#"); 
			return new Tuple2(split[0],split[1]);
		}
    	   
       });
       
       JavaPairRDD<String,Iterable<String>> combined = rddPair.groupByKey();
      System.out.println(combined);
      Map<String, Iterable<String>> comibinedMap = combined.collectAsMap();
      Iterator it = comibinedMap.entrySet().iterator();
      System.out.println("The Searched key " + args[1] + "is present in the following files :");
      while (it.hasNext()) {
          Map.Entry pair = (Map.Entry)it.next();
          System.out.print("File : " + pair.getKey() +" in the lines :- " );
          Iterator<String> itr = ((Iterable)pair.getValue()).iterator();
	      while(itr.hasNext()){
	    	  System.out.print(itr.next() + ",");
	      }
	      System.out.println();
	      it.remove(); // avoids a ConcurrentModificationException
      }
       
   
   
    }
    public static void main( String[] args )
    {
        if( args.length == 0 )
        {
            System.out.println( "Need Input Arguments" );
            System.exit( 0 );
        }

        invertSpark( args );
    }
}	
