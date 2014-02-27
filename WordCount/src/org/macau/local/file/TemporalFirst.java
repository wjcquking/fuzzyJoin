package org.macau.local.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.macau.flickr.temporal.TemporalUtil;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;

public class TemporalFirst {

	public static final long TEMPORAL_THRESHOLD = 70L*24*3600*1000;
	
	public static void main(String[] args){
		ArrayList<FlickrData> records = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.dataPath);
//		
//		Map<Long,ArrayList<FlickrData>> map = new HashMap<Long,ArrayList<FlickrData>>();
//		
//		for(FlickrData record: records){
//			
//			long time = record.getTimestamp() / TemporalUtil.MS_OF_ONE_DAY;
//			if(map.get(time) == null){
//				ArrayList<FlickrData> datas =  new ArrayList<FlickrData>();
//				datas.add(record);
//				map.put(time, datas);
//			}else{
//				map.get(time).add(record);
//			}
//			
//		}
//		
		int size = 100;
		Long startTime = System.currentTimeMillis();
		int compareCount = 0;
		int count = 0;
		int count2 = 0;
		for (int i = 0; i < size; i++) {
			FlickrData rec1 = records.get(i);
		    for (int j = i + 1; j < size; j++) {
		    	compareCount++;
		    	
		    	FlickrData rec2 = records.get(j);
		    	long ridA = rec1.getId();
	            long ridB = rec2.getId();
	            System.out.println(i + ":" + j + ":" + (float)Math.abs(rec1.getTimestamp()- rec2.getTimestamp())/(24*3600*1000));
	            System.out.println(70*24*3600*1000 + ":"+Math.abs(rec1.getTimestamp()- rec2.getTimestamp()) + ":" + TEMPORAL_THRESHOLD + ":" + (Math.abs(rec1.getTimestamp()- rec2.getTimestamp()) < TEMPORAL_THRESHOLD));
		    	if(FlickrSimilarityUtil.TemporalSimilarity(rec1, rec2)){
		    		System.out.println(count);
		            count++;
		    	}
		    	if(Math.abs(rec1.getTimestamp()- rec2.getTimestamp()) < TEMPORAL_THRESHOLD){
		    		System.out.println(count2);
		    		count2++;
		    	}
		    	
		    }
		}
		System.out.println(compareCount +":" + size*(size-1)/2);
		System.out.println(count + ":" + (double)count/compareCount);
		System.out.println("Phase One cost"+ (System.currentTimeMillis() -startTime)/ (float) 1000.0 + " seconds.");
	}
	
}
