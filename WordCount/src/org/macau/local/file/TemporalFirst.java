package org.macau.local.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.macau.flickr.temporal.TemporalUtil;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.local.util.FlickrData;
import org.macau.local.util.FlickrDataLocalUtil;

public class TemporalFirst {

	public static void main(String[] args){
		ArrayList<FlickrData> records = ReadFlickrData.readFileByLines(FlickrDataLocalUtil.dataPath);
		
		Map<Long,ArrayList<FlickrData>> map = new HashMap<Long,ArrayList<FlickrData>>();
		
		for(FlickrData record: records){
			
			long time = record.getTimestamp() / TemporalUtil.MS_OF_ONE_DAY;
			if(map.get(time) == null){
				ArrayList<FlickrData> datas =  new ArrayList<FlickrData>();
				datas.add(record);
				map.put(time, datas);
			}else{
				map.get(time).add(record);
			}
			
		}
		
		int count = 0;
		for (int i = 0; i < records.size(); i++) {
			FlickrData rec1 = records.get(i);
		    for (int j = i + 1; j < records.size(); j++) {
		    	FlickrData rec2 = records.get(j);
		    	long ridA = rec1.getId();
	            long ridB = rec2.getId();
		    	if(FlickrSimilarityUtil.TemporalSimilarity(rec1, rec2)){
		    		
		            count++;
		    	}
		    	
		    }
		    System.out.println(i+ ":"+count);
		}
		
	}
	
}
