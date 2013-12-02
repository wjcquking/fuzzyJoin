package org.macau.flickr.knn.exact.first;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.macau.flickr.util.FlickrSimilarityUtil;

public class PartitionMapper extends
	Mapper<Object,Text,IntWritable,IntWritable>{
	
	private final IntWritable outputKey = new IntWritable();
	
	private final static IntWritable one = new IntWritable(1);

	public static int tileNumber(double lat,double lon){
		
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE);
		return latNumber + lonNumber* FlickrSimilarityUtil.TILE_NUMBER_EACH_LINE;
	}
	
	/**
	 * the setup function load the pivot list
	 */
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("setup");
	}
	
	public void map(Object key,Text value,Context context)
		throws IOException, InterruptedException{
		

		double lat = Double.parseDouble(value.toString().split(";")[1]);
		
		double lon = Double.parseDouble(value.toString().split(";")[2]);
		
		
		outputKey.set(tileNumber(lat,lon));
		
		if(tileNumber(lat,lon) == 149){
			System.out.println(value);
		}
		
		context.write(outputKey,one);
	}
}
