package org.macau.flickr.spatial.minimal;

/**
 * @author: wangjian
 * the idea comes from the paper "Minimal MapReduce Algorithms",SIGMOD, 2013
 * 
 * Function: Sample data in a probability and send the sample data to all the other machine
 */


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.spatial.ZOrderValue;
import org.macau.flickr.spatial.partition.*;

/**
 * 
 * @author mb25428
 * Read the flickr data, Random sample to decide whether send to other machine
 * if ture, then extract the spatial information, send the object to other machines
 * For Example
 * The Data form:ID;lat;lon;timestamp
 * The Data Example:1093113743;48.89899;2.380696;973929974000
 */
public class MiniSpatialSampleMapper extends
Mapper<Object, Text, IntWritable, FlickrValue>{


	private static final double sampleProbability = 0.04;
	
	public static int tileNumber(double lat,double lon){
		
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.tilesNumber);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.tilesNumber);
		return ZOrderValue.parseToZOrder(latNumber, lonNumber);
		
	}
	
	public static int paritionNumber(int tileNumber){
		
		return (tileNumber +1) % FlickrSimilarityUtil.partitionNumber;
		
	}
	
	private IntWritable outputKey = new IntWritable();
	private final FlickrValue outputValue = new FlickrValue();
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		long id =Long.parseLong(value.toString().split(";")[0]);
		double lat = Double.parseDouble(value.toString().split(";")[1]);
		double lon = Double.parseDouble(value.toString().split(";")[2]);
		long timestamp = Long.parseLong(value.toString().split(";")[3]);
		

		
		int  tileNumber = tileNumber(lat,lon);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tileNumber);
		outputValue.setTimestamp(timestamp);
		
		outputKey.set(GridPartition.paritionNumber(tileNumber));
		context.write(outputKey, outputValue);
		
	}
}
