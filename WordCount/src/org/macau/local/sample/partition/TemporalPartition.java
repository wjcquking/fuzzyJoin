package org.macau.local.sample.partition;

import java.util.Iterator;
import java.util.Map;

import org.macau.local.sample.SpatialBlackBoxWR2;
import org.macau.local.util.FlickrDataLocalUtil;

/**
 * 
 * @author mb25428
 * This class is to get the histogram of the two data set and 
 * 
 */


public class TemporalPartition {
	public static void main(String[] args){
		System.out.println("The Temporal Partition");
		verifyHistogramWithSelectivity();
	}
	
	/**
	 * This function is to verify that the result of histogram is same to 
	 * the candidate selectivity result
	 * 
	 * R * S * rho = r1*s1 + r2*s2 + ... + rn*sn
	 */
	public static void verifyHistogramWithSelectivity(){
		System.out.println("read the histogram");
		
		Map<Integer,Integer> rAccountMap = SpatialBlackBoxWR2.getTemporalWeightedData(FlickrDataLocalUtil.rDataPath);
		Map<Integer,Integer> sAccountMap = SpatialBlackBoxWR2.getTemporalWeightedData(FlickrDataLocalUtil.sDataPath);
		
		Iterator iter = rAccountMap.entrySet().iterator(); 
		double sum = 0;
		while (iter.hasNext()) { 
		    Map.Entry entry = (Map.Entry) iter.next(); 
		    Object key =  entry.getKey();
		    Integer val = (Integer) entry.getValue();
		    if(sAccountMap.get(key) != null){
		    	sum += val * sAccountMap.get(key);
		    }
		}		
		System.out.println(sum);
		System.out.println("the selectivity is " + sum / 10000 / 10000);
		
	}
	
	
}
