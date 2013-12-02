package org.macau.flickr.knn.exact.first;

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
import org.macau.flickr.util.FlickrSimilarityUtil;


public class PartitionJob {

	public static boolean spatialAccountJob(Configuration conf) throws Exception{
		
		Job accountJob = new Job(conf,"Spatial Account Job");
		
		accountJob.setJarByClass(PartitionJob.class);
		
		accountJob.setMapperClass(PartitionMapper.class);
		accountJob.setCombinerClass(PartitionReducer.class);
		accountJob.setReducerClass(PartitionReducer.class);
		
		accountJob.setOutputKeyClass(IntWritable.class);
		accountJob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(accountJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(accountJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(accountJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
