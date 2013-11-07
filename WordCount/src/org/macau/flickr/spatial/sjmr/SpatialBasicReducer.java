package org.macau.flickr.spatial.sjmr;

/**
 * There are two step
 * Filter Step: Input: the tuples belongs to the same partitions
 * the goal is to find the paris of intersecting rectangles between the two sets by strip-based plane sweeping techniques.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class SpatialBasicReducer extends
	Reducer<IntWritable, FlickrValue, Text, Text>{

	private final Text text = new Text();
	private final Map<Integer,ArrayList<FlickrValue>> map = new HashMap<Integer,ArrayList<FlickrValue>>();
	//private final ArrayList<FlickrValue> records = new ArrayList<FlickrValue>();
	
	public void reduce(IntWritable key, Iterable<FlickrValue> values,
			Context context) throws IOException, InterruptedException{
		
		int count = 0;
		for(FlickrValue value:values){
			count++;
			if(map.containsKey(value.getTag())){
				map.get(value.getTag()).add(value);

			}else{
				
				//System.out.println("2"+ value.getTag());
				ArrayList<FlickrValue> recordList = new ArrayList<FlickrValue>();
				recordList.add(value);
				map.put(new Integer(value.getTag()),recordList);
			}
		}
		
		System.out.println(key + ":" + count);

		
		Iterator it = map.entrySet().iterator();
		
		//System.out.println("size is "+map.size());
		
		while(it.hasNext()){
			Map.Entry<Integer,ArrayList<FlickrValue>> m = (Map.Entry<Integer,ArrayList<FlickrValue>>)it.next();
			m.getKey();
			ArrayList<FlickrValue> records = m.getValue();
			
//			System.out.println(m.getKey() +" "+ m.getValue().size());
			//brute force
//			for (int i = 0; i < records.size(); i++) {
//				FlickrValue rec1 = records.get(i);
//			    for (int j = i + 1; j < records.size(); j++) {
//			    	FlickrValue rec2 = records.get(j);
//			    	
//			    	if(FlickrSimilarityUtil.SpatialSimilarity(rec1, rec2)){
//			    		long ridA = rec1.getId();
//			            long ridB = rec2.getId();
////			            if (ridA < ridB) {
////			                long rid = ridA;
////			                ridA = ridB;
////			                ridB = rid;
////			            }
//			            
////			            System.out.println(ridA + "%" + ridB);
//			            text.set(ridA + "%" + ridB);
//			            context.write(text, new Text(""));
//			    	}
//			    	
//			    }
//			}
		}
		
		//System.out.println("key is " + key + "record size is " + records.size());
		
		
	}
	
}
