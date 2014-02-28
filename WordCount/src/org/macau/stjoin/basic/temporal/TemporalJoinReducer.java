package org.macau.stjoin.basic.temporal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

import org.macau.stjoin.basic.temporal.TemporalComparator;;

public class TemporalJoinReducer extends
	Reducer<LongWritable, FlickrValue, Text, Text>{
		
		
		private final Text text = new Text();
//		private final ArrayList<FlickrValue> records = new ArrayList<FlickrValue>();
		
		private final Map<Integer,ArrayList<FlickrValue>> rMap = new HashMap<Integer,ArrayList<FlickrValue>>();
		private final Map<Integer,ArrayList<FlickrValue>> sMap = new HashMap<Integer,ArrayList<FlickrValue>>();
		
		@SuppressWarnings("unchecked")
		public void reduce(LongWritable key, Iterable<FlickrValue> values,
				Context context) throws IOException, InterruptedException{
			
			
			for(FlickrValue value:values){
				
//				FlickrValue recCopy = new FlickrValue(value);
//			    records.add(recCopy);
			    
//			    System.out.println(value);
			    
			    if(value.getTag() == FlickrSimilarityUtil.R_tag){
			    	
			    	if(rMap.containsKey(value.getTileNumber())){
				    	
				    	rMap.get(value.getTileNumber()).add(new FlickrValue(value));
				    	
				    }else{
				    	
				    	ArrayList<FlickrValue> list = new ArrayList<FlickrValue>();
				    	
				    	list.add(value);
//				    	System.out.println("rmap:" + rMap.size());
				    	
				    	rMap.put(value.getTileNumber(), list);
//				    	System.out.println("rmap==" + rMap.size());
				    }
			    }else{
			    	
			    	if(sMap.containsKey(value.getTileNumber())){
				    	
				    	sMap.get(value.getTileNumber()).add(new FlickrValue(value));
				    	
				    }else{
				    	
				    	ArrayList<FlickrValue> list = new ArrayList<FlickrValue>();
				    	
				    	list.add(value);
				    	
				    	sMap.put(value.getTileNumber(), list);
				    }
			    }
			    
			    
			}
			
		
//			System.out.println("smap1**" + sMap.size());
//			System.out.println("rmap1**" + rMap.size());
			
			for(java.util.Iterator<Integer> i = rMap.keySet().iterator();i.hasNext();){
				
				TemporalComparator comp = new TemporalComparator();
				Collections.sort(rMap.get(i.next()),comp);
				
				
			}
			

			
			
			for(java.util.Iterator<Integer> i = sMap.keySet().iterator();i.hasNext();){
				
				TemporalComparator comp = new TemporalComparator();
				Collections.sort(sMap.get(i.next()),comp);
				
			}
			
//			System.out.println("smap2** " + sMap.size());
//			System.out.println("rmap2** " + rMap.size());
			
			for(java.util.Iterator<Integer> obj = rMap.keySet().iterator();obj.hasNext();){
				
//				System.out.println("smap3**" + sMap.size());
//				System.out.println("rmap3**" + rMap.size());
				Integer i = obj.next();
				
				
				for(java.util.Iterator<Integer> o= sMap.keySet().iterator();o.hasNext();){
					
					int value = o.next();
					
				}
				
				if(sMap.containsKey(i)){
					
					
					for(int j = 0;j < rMap.get(i).size();j++){
						
						FlickrValue value1 = rMap.get(i).get(j);
						
//						System.out.println(i + "," + j );
						
						for(int k = 0; k < sMap.get(i).size();k++){
							FlickrValue value2 = sMap.get(i).get(k);
							
							if(FlickrSimilarityUtil.SpatialSimilarity(value1, value2)){
								long ridA = value1.getId();
					            long ridB = value2.getId();
					            if (ridA < ridB) {
					                long rid = ridA;
					                ridA = ridB;
					                ridB = rid;
					            }
					            
					            //System.out.println("" + ridA + "%" + ridB);
					            
					            text.set("" + ridA + "%" + ridB);
					            context.write(text, new Text(""));
							}
						}
						
						// for the adjacent tail
						if(sMap.containsKey(i+1)){
							for(int m = 0; m < sMap.get(i+1).size();m++){
								FlickrValue value3 = sMap.get(i+1).get(m);
								
								if(FlickrSimilarityUtil.SpatialSimilarity(value1, value3)){
									
									if(FlickrSimilarityUtil.SpatialSimilarity(value1, value3) && FlickrSimilarityUtil.TextualSimilarity(value1, value3)){
										
										
										long ridA = value1.getId();
							            long ridB = value3.getId();
							            
							            if (ridA < ridB) {
							                long rid = ridA;
							                ridA = ridB;
							                ridB = rid;
							            }
							            
										text.set("" + ridA + "%" + ridB);
							            context.write(text, new Text(""));
									}
								}
								
							}
						}
						
					}
				}
			}

			
			rMap.clear();
			sMap.clear();
		}
	}

