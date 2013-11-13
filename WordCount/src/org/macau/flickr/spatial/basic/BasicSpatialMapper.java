package org.macau.flickr.spatial.basic;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

/**
 * 
 * @author mb25428
 * Read the flickr data, extract the spatial information
 * For Example
 * The Data form:
 * ID;lat;lon;timestamp
 * 1093113743;48.89899;2.380696;973929974000
 */
public class BasicSpatialMapper extends
Mapper<Object, Text, IntWritable, FlickrValue>{

	//use the Hilbert curve to find the best partition function
	
	
	//get the whole universe and get the tile number
	public static int tileNumber(double lat,double lon){

		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.tilesNumber);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.tilesNumber);
		return latNumber + lonNumber* FlickrSimilarityUtil.tilesNumber;
	}
	
	public static int paritionNumber(int tileNumber){
		
		int number = 1;
		return number;
	}
	
	private IntWritable outputKey = new IntWritable();
	private final FlickrValue outputValue = new FlickrValue();
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		long id =Long.parseLong(value.toString().split(";")[0]);
		double lat = Double.parseDouble(value.toString().split(";")[1]);
		double lon = Double.parseDouble(value.toString().split(";")[2]);
		long timestamp = Long.parseLong(value.toString().split(";")[3]);
		
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTimestamp(timestamp);
		
		outputKey.set(tileNumber(lat,lon));
		
		context.write(outputKey, outputValue);
		
	}
}
