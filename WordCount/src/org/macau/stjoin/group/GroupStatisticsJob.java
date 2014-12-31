package org.macau.stjoin.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.macau.flickr.job.TemporalSimilarityJoin;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;


/**
 * 
 * @author hadoop
 * Map: Input:  KEY  : 
 *              Value:
 *      output: KEY  :
 *      		Value:
 *      
 * Reduce: Input: KEY  :
 * 				  Value:
 */
public class GroupStatisticsJob {

	public static boolean TemporalSimilarityBasicJoin(Configuration conf) throws Exception{
		
		Job basicJob = new Job(conf,"Temporal Basic Similarity Join");
		basicJob.setJarByClass(TemporalSimilarityJoin.class);
		
		basicJob.setMapperClass(GroupStatisticsMapper.class);
//		basicJob.setCombinerClass(TemporalJoinReducer.class);
		
//		basicJob.setReducerClass(GroupStatisticsReducer.class);
		
		basicJob.setMapOutputKeyClass(Text.class);
		basicJob.setMapOutputValueClass(Text.class);
		
//		basicJob.setOutputKeyClass(Text.class);
//		basicJob.setOutputValueClass(Text.class);
		basicJob.setNumReduceTasks(6);
		
		FileInputFormat.addInputPath(basicJob, new Path(FlickrSimilarityUtil.flickrInputPath));
		FileOutputFormat.setOutputPath(basicJob, new Path(FlickrSimilarityUtil.flickrOutputPath));
		
		if(basicJob.waitForCompletion(true))
			return true;
		else
			return false;
	}
}
