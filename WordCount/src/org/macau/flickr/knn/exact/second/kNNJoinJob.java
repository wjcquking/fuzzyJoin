package org.macau.flickr.knn.exact.second;

/**
 * Simple MapReduce Job which can find the range of the spatial data
 * and the number of each location point
 * for the paris flickr data, I analyze the longitude and latitude range
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.knn.exact.first.kNNPartition;
import org.macau.flickr.knn.util.kNNUtil;
import org.macau.flickr.util.FlickrSimilarityUtil;


public class kNNJoinJob {

	public static kNNPartition[] R_Partition = new kNNPartition[kNNUtil.REDUCER_NUMBER];
	public static kNNPartition[] S_Partition = new kNNPartition[kNNUtil.REDUCER_NUMBER];
	
	
	public static boolean spatialPartitionjob(Configuration conf) throws Exception{
		
		
		Job job = new Job(conf,"kNN exact Join Job");
		
		job.setJarByClass(kNNJoinJob.class);
		
		for(int i = 0; i < kNNUtil.REDUCER_NUMBER;i++){
			
			R_Partition[i] = new kNNPartition(i,0);
			
			S_Partition[i] = new kNNPartition(i,0);
		}
		
		job.setMapperClass(kNNJoinMapper.class);
//		job.setCombinerClass(PartitionReducer.class);
		job.setReducerClass(kNNJoinReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(FlickrSimilarityUtil.flickrOutputPath));
		FileOutputFormat.setOutputPath(job, new Path(FlickrSimilarityUtil.flickrResultPath));
		
		if(job.waitForCompletion(true)){
			int R_Sum = 0;
			int S_Sum = 0;
			for(int i = 0;i < kNNUtil.REDUCER_NUMBER;i++){
				R_Sum+= R_Partition[i].getCount();
				S_Sum += S_Partition[i].getCount();
				
				System.out.println(i + ":" + R_Partition[i].getCount()+","+ R_Partition[i].getMinDistance()+","+ R_Partition[i].getMaxDistance() + ";" + S_Partition[i].getCount()+","+ S_Partition[i].getMinDistance()+","+ S_Partition[i].getMaxDistance());
			}
			
			System.out.println("R:" + R_Sum + ";S:" + S_Sum);
			
			return true;
		}
		else
			return false;
	}
}
