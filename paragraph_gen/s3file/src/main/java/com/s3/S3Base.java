package com.s3;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3; 
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * List Objects in S3 bucket
 */
public class S3Base {

  AmazonS3 s3;

  public void init(String accessKey, String secretKey) {
    s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
  }

  public void listAllBuckets() {
    // List all the buckets
    List<Bucket> buckets = s3.listBuckets();
    for (Bucket next : buckets) {
      //System.out.println(next.getName());
      listBucketContent(next.getName());
    }
  }
//http://docs.aws.amazon.com/AmazonS3/latest/dev/RetrievingObjectUsingJava.html
  public void listBucketContent(String bucketName) {
    
	  ObjectListing listing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
    for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {
    	
    	String key=objectSummary.getKey();
    	String [] splitkey=key.split("/");
    	//reach the output folder, dirindexoutput is reducer o/p folder
    	if((splitkey.length>0)&& ( splitkey[0].equals("dirindexoutput"))&& (!(splitkey[1]).equalsIgnoreCase("_SUCCESS"))){
    	System.out.println(" -> " + key + "  " +
    			"(size = " + objectSummary.getSize()/1024 + " KB)");
    	System.out.println(objectSummary.getBucketName());
    	
      }
    }
  }

  //reading a file from s3
  public void readfile(String bucketName, String key,long offset) throws IOException{
	  
	  /*S3Object s3object = s3.getObject(new GetObjectRequest(
	    		bucketName, key));
	    System.out.println("Content-Type: "  + 
	    		s3object.getObjectMetadata().getContentType());
	    
	    	
	    displayTextInputStream(s3object.getObjectContent());
	    */
	  
	  	GetObjectRequest rangeObjectRequest = new GetObjectRequest(
				bucketName, key);
		rangeObjectRequest.setRange(offset -256,offset +256 ); // retrieve +-256 bytes. from the file
		S3Object objectPortion = s3.getObject(rangeObjectRequest);

		InputStream objectData = objectPortion.getObjectContent();
		// Process the objectData stream.
		displayTextInputStream(objectData);
		
		objectData.close();
  }
  
  
  
  public static void main(String[] args) throws IOException {
    
	  
	  S3Base base = new S3Base();
   // base.init(args[0], args[1]);
	  base.init("AKIAJHYTE7CSSI66LS5Q", "AFzLYhI9LWW9jU1I59NXSTIORj+eJcxSbbVtLal4");
   // base.listAllBuckets();
	  
   // base.readfile("281invindexlogging","smalldirinput/smallfiles/3/0/6/0/30601/30601.txt",Long.parseLong(args[0]));
    base.readfile("281invindexlogging",args[0],Long.parseLong(args[1]));
  }
  private static void displayTextInputStream(InputStream input)
		    throws IOException {
		    	// Read one text line at a time and display.
		        BufferedReader reader = new BufferedReader(new 
		        		InputStreamReader(input));
		        while (true) {
		            String line = reader.readLine();
		            if (line == null) break;	            
		            System.out.println("    " + line);
		        }
		        System.out.println();
		    }		
}

