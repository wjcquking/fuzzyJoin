package org.macau.flickr.knn.exact.first;

/**
 * Simple MapReduce Job which can find the range of the spatial data
 * and the number of each location point
 * for the paris flickr data, I analyze the longitude and latitude range
 */

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.macau.flickr.knn.util.kNNUtil;
import org.macau.flickr.util.FlickrSimilarityUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class PartitionJob {

	public static kNNPartition[] R_Partition = new kNNPartition[kNNUtil.REDUCER_NUMBER];
	public static kNNPartition[] S_Partition = new kNNPartition[kNNUtil.REDUCER_NUMBER];
	
	public static boolean spatialPartitionjob(Configuration conf) throws Exception{
		
		
		Job job = new Job(conf,"kNN exact Join Job");
		
		job.setJarByClass(PartitionJob.class);
		
		for(int i = 0; i < kNNUtil.REDUCER_NUMBER;i++){
			
			R_Partition[i] = new kNNPartition(i,0);
			
			S_Partition[i] = new kNNPartition(i,0);
		}
		
		// there is only map function, no need the reducer function.
		job.setMapperClass(PartitionMapper.class);
//		job.setCombinerClass(PartitionReducer.class);
//		job.setReducerClass(PartitionReducer.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(job, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(job.waitForCompletion(true)){
			/*
			int R_Sum = 0;
			int S_Sum = 0;
			for(int i = 0;i < kNNUtil.REDUCER_NUMBER;i++){
				R_Sum+= R_Partition[i].getCount();
				S_Sum += S_Partition[i].getCount();
				
				System.out.println(S_Partition[i].getLat() + ";"+ S_Partition[i].getLon()+ ";" + R_Partition[i].getLat() + ";"+R_Partition[i].getLon());
//				System.out.println(i + ":" + R_Partition[i].getCount()+","+ R_Partition[i].getMinDistance()+","+ R_Partition[i].getMaxDistance() + ";" + S_Partition[i].getCount()+","+ S_Partition[i].getMinDistance()+","+ S_Partition[i].getMaxDistance());
				for(int j = 0; j < kNNUtil.k;j++){
//					System.out.print(S_Partition[i].getkNNDistance().get(j) + "  ");
				}
				System.out.println();
			}
			
			System.out.println("R:" + R_Sum + ";S:" + S_Sum);
			*/
			/*
			 * The best way is that each mapper write the statistic to the DFS
			 * and another program read the files from the DFS and get the statistic
			 */
		
			
			
			
			return true;
		}
		else
			return false;
	}
}
