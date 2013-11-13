package org.macau.flickr.spatial.minimal;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
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

public class MiniSpatialMapper extends
Mapper<Object, Text, DoubleWritable, FlickrValue>{

	
	

	
	
	/*
	 * First, each machine use the random sampling algorithm to get the sample
	 * to get the boundary object
	 * Second, order the object according one dimension
	 * So I choose the lat dimension
	 */
	public static int tileNumber(double lat,double lon){
		int latNumber = (int) ((lat - FlickrSimilarityUtil.MIN_LAT)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.tilesNumber);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.MIN_LON)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.tilesNumber);
		return latNumber + lonNumber* FlickrSimilarityUtil.tilesNumber;
	}
	
	/**
	 * 
	 * @param lat
	 * @return the machine number
	 * load the boundary set to decide the machine number which the object should be send to 
	 * 
	 */
	public static int paritionNumber(double lat){
		
		int number = 1;
		return number;
	}
	
	private DoubleWritable outputKey = new DoubleWritable();
	private final FlickrValue outputValue = new FlickrValue();
	
	/**
	 * read the data from the local data
	 */
	private void loadBoundarySet(){
		
	}
	
	
	/**
	 * get the boundary data set
	 * and send the data to different 
	 */
	protected void setup(Context context) throws IOException, InterruptedException {

	}
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
		
		outputKey.set(lat);
		
		context.write(outputKey, outputValue);
		
	}
}
