package org.macau.flickr.knn.util;

public class kNNUtil {

	/**
	 * exact kNN
	 */
	//reducer number
	public static final int REDUCER_NUMBER = 60;
	public static final double probability = 0.1;
	
	public static final String  R_FILE_PATH = "//home//hadoop//Dropbox//hadoop//input//flickr.even.data";
	public static final String R_RANDOM_PATH = "//home//hadoop//Dropbox//hadoop//input//flickr.random.data";
	public static final String R_FARTHEST_PATH = "//home//hadoop//Dropbox//hadoop//input//flickr.farthest.data";
	public static final String R_KMEANS_PATH = "//home//hadoop//Dropbox//hadoop//input//flickr.kmeans.data";
	
	//random selection
	public static final int RANDOM_SELECTION_T = 20;
	public static final double RANDOM_SELECTION_PROBABILITY = 0.005;
	
	
	//farthest selection
	public static final double FARTHEST_SELECTION_PROBABILITY = 0.0005;
	
	//k-means selection
	public static final double KMEANS_SELECTION_PROBABILITY = 0.005;
	
	/**
	 * approximate kNN
	 */
	//approximate util value
	// the vector number
	public static int alpha = 5;
	
	//kNN number
	public static int k = 5;
	
	//calculate the sample probability which equal = 1/(epsilon^2 * N)
	public static double epsilon = 0.01;
}
