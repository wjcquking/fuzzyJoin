package org.macau.flickr.util;

import org.macau.spatial.Distance;

public class FlickrSimilarityUtil {

	//time threshold
	public static long temporalThreshold = 24*3600*1000;
	
	//spatial threshold, Unit : km
	public static double distanceThreshold = 0.002;
	 
	//textual threshold
	public static double textualThreshold = 0.6;
	
	//three machine and each has two reduces
	public static final int reducerNumber = 6;
	
	//the tile number of each line
	public static final int tilesNumber = 12;
	
	public static final int totalTileNumber = tilesNumber * tilesNumber;
	
	public static final int partitionNumber = 6;
	
	/* For the data of Paris flickr image picture
	 * If We know the data,we can split the whole universe
	*/
	public static double maxLat = 48.902967;
	public static double maxLon = 2.473817;
	public static double minLat = 48.815101;
	public static double minLon = 2.223266;
	
	public static double wholeSpaceWidth = maxLat - minLat;
	public static double WholeSpaceLength = maxLon - minLon;
	
	
	
	public static final String flickrInputPath = "hdfs://localhost:9000/user/hadoop/input";
	public static final String flickrOutputPath = "hdfs://localhost:9000/user/hadoop/output";
//	public static final String flickrInputPath = "hdfs://10.1.1.1:10000/user/hadoop/flickr/input";
//	public static final String flickrOutputPath = "hdfs://10.1.1.1:10000/user/hadoop/flickr/output";
	
	
	public static boolean TemporalSimilarity(FlickrValue value1,FlickrValue value2){
		//System.out.println("time " + (value1.getTimestamp()- value2.getTimestamp()));
		return Math.abs(value1.getTimestamp()- value2.getTimestamp()) < temporalThreshold;
	}
	
	/**
	 * 
	 * @param value1
	 * @param value2
	 * @return if the distance of two objects is larger than the distance threshold, return true, else return false
	 */
	public static boolean SpatialSimilarity(FlickrValue value1, FlickrValue value2){
		return Distance.GreatCircleDistance(value1.getLat(), value1.getLon(), value2.getLat(), value2.getLon()) < distanceThreshold;
	}
	
	public static double SpatialDistance(FlickrValue value1, FlickrValue value2){
		return Distance.GreatCircleDistance(value1.getLat(), value1.getLon(), value2.getLat(), value2.getLon());
	}
}
