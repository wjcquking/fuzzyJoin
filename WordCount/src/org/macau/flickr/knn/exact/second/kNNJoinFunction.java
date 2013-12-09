package org.macau.flickr.knn.exact.second;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.macau.flickr.knn.exact.first.kNNPartition;
import org.macau.flickr.knn.util.kNNUtil;
import org.macau.spatial.Distance;

public class kNNJoinFunction {

	/**
	 * 
	 * @param R_Partition
	 * @param S_Partition
	 * get the theta value for each partition in R.
	 */
	public static double boundingkNN(kNNPartition R_Partition,kNNPartition[] S_Partition){
			
		List<Double> topList = new ArrayList<Double>();
		
		for(int j = 0; j < kNNUtil.REDUCER_NUMBER;j++){
			
			for(int k = 0; k < kNNUtil.REDUCER_NUMBER;k++){
				
				double distance = Distance.GreatCircleDistance(R_Partition.getLat(),  R_Partition.getLon(),  S_Partition[j].getLat(), S_Partition[j].getLon());
				double upperBound = R_Partition.getMaxDistance() + distance + S_Partition[j].getkNNDistance().get(k);
				
				if(topList.size() < kNNUtil.REDUCER_NUMBER){
					
					topList.add(upperBound);
					Collections.sort(topList);
					
				}else if(topList.get(kNNUtil.REDUCER_NUMBER-1) > upperBound){
					
					topList.remove(kNNUtil.REDUCER_NUMBER-1);
					topList.add(upperBound);
					Collections.sort(topList);
				}
						
			}
		}
		
		return topList.get(kNNUtil.REDUCER_NUMBER-1);
	
	}
	
	/**
	 * 
	 * @param R_Partition
	 * @param S_Partition
	 * get the theta value for each partition in R.
	 */
	public static void boundingkNN(kNNPartition[] R_Partition,kNNPartition[] S_Partition){
		
		List<Double> thetaList = new ArrayList<Double>();
		
		for(int i = 0; i < kNNUtil.REDUCER_NUMBER;i++){
			
			List<Double> topList = new ArrayList<Double>();
			
			for(int j = 0; j < kNNUtil.REDUCER_NUMBER;j++){
				
				for(int k = 0; k < kNNUtil.REDUCER_NUMBER;k++){
					
					double distance = Distance.GreatCircleDistance(R_Partition[i].getLat(),  R_Partition[i].getLon(),  S_Partition[j].getLat(), S_Partition[j].getLon());
					double upperBound = R_Partition[i].getMaxDistance() + distance + S_Partition[j].getkNNDistance().get(k);
					
					if(topList.size() < kNNUtil.REDUCER_NUMBER){
						
						topList.add(upperBound);
						Collections.sort(topList);
						
					}else if(topList.get(kNNUtil.REDUCER_NUMBER-1) > upperBound){
						
						topList.remove(kNNUtil.REDUCER_NUMBER-1);
						topList.add(upperBound);
						Collections.sort(topList);
					}
							
				}
			}
			
			thetaList.add(topList.get(kNNUtil.REDUCER_NUMBER-1));
		}
	}
}
