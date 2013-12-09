package org.macau.flickr.util;

import org.macau.spatial.Distance;

public class FlickrSimilarityUtil {

	//time threshold
	public static final long TEMPORAL_THRESHOLD = 24*3600*1000;
	
	//spatial threshold, Unit : km
	public static final double DISTANCE_THRESHOLD = 0.001;
	 
	//textual threshold
	public static final double TEXTUAL_THRESHOLD = 0.6;
	
	
	public static final double SAMPLE_PROBABILITY = 0.004;
	
	public static final int MACHINE_NUNBER = 3;
	//three machine and each has two reduces
	public static final int REDUCER_NUMBER = 6;
	
	//the tile number of each line
	public static final int TILE_NUMBER_EACH_LINE = 100;
	
	public static final int TOTAL_TILE_NUMBER = TILE_NUMBER_EACH_LINE * TILE_NUMBER_EACH_LINE;
	
	public static final int PARTITION_NUMBER = 6;
	
	/* For the data of Paris flickr image picture
	 * If We know the data,we can split the whole universe
	*/
	public static final double MAX_LAT = 48.902967;
	public static final double MAX_LON = 2.473817;
	public static final double MIN_LAT = 48.815101;
	public static double MIN_LON = 2.223266;
	
	public static double wholeSpaceWidth = MAX_LAT - MIN_LAT;
	public static double WholeSpaceLength = MAX_LON - MIN_LON;
	
	public static final int R_tag = 0;
	public static final int S_tag = 1;
	
	public static final String R_TAG = "even";
	public static final String S_TAG = "odd";
	
	public static final String flickrInputPath = "hdfs://localhost:9000/user/hadoop/input";
	public static final String flickrOutputPath = "hdfs://localhost:9000/user/hadoop/output";
	public static final String flickrResultPath = "hdfs://localhost:9000/user/hadoop/result";
//	public static final String flickrInputPath = "hdfs://10.1.1.1:10000/user/hadoop/flickr/input";
//	public static final String flickrOutputPath = "hdfs://10.1.1.1:10000/user/hadoop/flickr/output";
	
	/**
	 * 
	 * @param value1
	 * @param value2
	 * @return if satisfy the similarity threshold, then return true, else return false
	 */
	public static boolean TemporalSimilarity(FlickrValue value1,FlickrValue value2){
		
		return Math.abs(value1.getTimestamp()- value2.getTimestamp()) < TEMPORAL_THRESHOLD;
		
	}
	
	/**
	 * 
	 * @param value1
	 * @param value2
	 * @return if the distance of two objects is larger than the distance threshold, return true, else return false
	 */
	public static boolean SpatialSimilarity(FlickrValue value1, FlickrValue value2){
		return Distance.GreatCircleDistance(value1.getLat(), value1.getLon(), value2.getLat(), value2.getLon()) < DISTANCE_THRESHOLD;
	}
	
	/**
	 * 
	 * @param value1
	 * @param value2
	 * @return the spatial distance between two FlickrValue
	 */
	public static double SpatialDistance(FlickrValue value1, FlickrValue value2){
		return Distance.GreatCircleDistance(value1.getLat(), value1.getLon(), value2.getLat(), value2.getLon());
	}
}
