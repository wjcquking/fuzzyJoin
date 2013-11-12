package org.macau.flickr.spatial.grid;

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
 * 
 * The solution:
 * Partition R in one way grid and pratition S in the extension grid, which means that one grid in R just need to 
 * compare one grid in the S, but there may be too many replications in S.
 */


public class GridSpatialMapper extends
Mapper<Object, Text, IntWritable, FlickrValue>{

	//get the whole universe and get the tile number,then order the tile according the hilbert value or z-order value
	public static int tileNumber(double lat,double lon){
		
		int latNumber = (int) ((lat - FlickrSimilarityUtil.minLat)/FlickrSimilarityUtil.wholeSpaceWidth * FlickrSimilarityUtil.tilesNumber);
		int lonNumber = (int)((lon- FlickrSimilarityUtil.minLon)/FlickrSimilarityUtil.WholeSpaceLength * FlickrSimilarityUtil.tilesNumber);
		return latNumber + lonNumber* FlickrSimilarityUtil.tilesNumber;
		
	}
	
	/**
	 * 
	 * @param tileNumber
	 * @return the parition Number, I use the round-robin function
	 */
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
