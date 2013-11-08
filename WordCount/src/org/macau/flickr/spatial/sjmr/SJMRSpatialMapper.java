package org.macau.flickr.spatial.sjmr;

/**
 * @author: wangjian
 * the idea comes from the paper "SJMR:Parallelizing Spatial Join with MapReduce",IEEE, 2009
 * 
 * The MapFunction
 */


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.flickr.util.spatial.ZOrderValue;

/**
 * 
 * @author mb25428
 * Read the flickr data, extract the spatial information
 * For Example
 * The Data form:
 * ID;lat;lon;timestamp
 * 1093113743;48.89899;2.380696;973929974000
 */
public class SJMRSpatialMapper extends
Mapper<Object, Text, IntWritable, FlickrValue>{

	/*
	 * use the z curve order and round-robin algorithm to find the best partition function
	 */

	//the universe is divided regularly into Nt tiles
	//each tile is number from 0 to Nt-1 according to z curve, 
	//and mapped to a partition p with a round robin scheme
	public static int tileNumber(double lat,double lon){
//		System.out.println(lat - FlickrSimilarityUtil.minLat);
//		System.out.println((lat - FlickrSimilarityUtil.minLat)/FlickrSimilarityUtil.wholeSpaceWidth);
//		System.out.println((lat - FlickrSimilarityUtil.minLat)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.tilesNumber);
		int latNumber = (int) ((lat - FlickrSimilarityUtil.minLat)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.tilesNumber);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.minLon)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.tilesNumber);
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
		
		if(id == 65480044){
			
		}
		
		
		int  tileNumber = tileNumber(lat,lon);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tileNumber);
		outputValue.setTimestamp(timestamp);
		
		outputKey.set(paritionNumber(tileNumber));
		context.write(outputKey, outputValue);
		
	}
}
