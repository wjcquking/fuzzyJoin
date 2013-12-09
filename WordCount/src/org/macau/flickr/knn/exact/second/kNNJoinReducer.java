package org.macau.flickr.knn.exact.second;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.macau.flickr.knn.exact.first.PartitionJob;
import org.macau.flickr.knn.exact.first.kNNPartition;
import org.macau.flickr.knn.util.kNNUtil;
import org.macau.flickr.util.FlickrPartitionValue;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.spatial.Distance;
import java.util.List;

public class kNNJoinReducer extends
	Reducer<IntWritable,FlickrPartitionValue,IntWritable,IntWritable>{

	private IntWritable result = new IntWritable();

	private static List<kNNPartition> S_SortPartition = new ArrayList<kNNPartition>();
	
	protected void setup(Context context) throws IOException, InterruptedException {
	
		System.out.println("kNN Join Reducer Starts");
	}
	
	
	public void reduce(IntWritable key,Iterable<FlickrPartitionValue> values, Context context)
		throws IOException,InterruptedException{
		int sum = 0;
		
		Map<Integer, Double> keyfreqs = new HashMap<Integer, Double>();

		List<FlickrPartitionValue> R_List = new ArrayList<FlickrPartitionValue>();
		
		
		for(int i = 0;i < kNNUtil.REDUCER_NUMBER;i++){
			
			double distance = Distance.GreatCircleDistance(PartitionJob.R_Partition[key.get()].getLat(), PartitionJob.R_Partition[key.get()].getLon(), PartitionJob.S_Partition[i].getLat(), PartitionJob.S_Partition[i].getLon());
			keyfreqs.put(i, distance);
			
		}
		ArrayList<Entry<Integer,Double>> SPartition = new ArrayList<Entry<Integer,Double>>(keyfreqs.entrySet());  
		
		Collections.sort(SPartition, new Comparator<Map.Entry<Integer, Double>>() {  
			
			public int compare(Map.Entry<Integer, Double> o1, Map.Entry<Integer, Double> o2) {  
				return Double.compare(o1.getValue(), o2.getValue());  
			}  
			
		});
		
		for(Entry<Integer,Double> e : SPartition) {
			System.out.println(e.getKey() + "::::" + e.getValue());
		}
		
		Map<Integer,List<FlickrPartitionValue>> sortParitionMap = new HashMap<Integer,List<FlickrPartitionValue>>();
		List<FlickrPartitionValue> kNNList = new ArrayList<FlickrPartitionValue>();
		//
		for(FlickrPartitionValue val : values){
			
			//R set
			if(val.getDataset() == FlickrSimilarityUtil.R_tag){
				
				R_List.add(val);
				
			}else{
				//S set: 1
				sortParitionMap.get(val.getPid()).add(val);
				if(S_SortPartition.get(val.getPid()) == null){
					S_SortPartition.add(new kNNPartition(val.getPid(),FlickrSimilarityUtil.REDUCER_NUMBER));
					S_SortPartition.get(val.getPid()).getkNNDistance().add(val.getDistance());
				}else{
					S_SortPartition.get(val.getPid()).getkNNDistance().add(val.getDistance());
				}
			}
		}
		
		
		for(FlickrPartitionValue r: R_List){
			double theta = 0;
			for(Entry<Integer,Double> e : SPartition) {
				sortParitionMap.get(e.getKey());
				
				for(FlickrPartitionValue s: sortParitionMap.get(e.getKey())){
					
					// if(Distance.GreatCircleDistance())
				}
			}
		}
		result.set(sum);
		context.write(key, result);
	}
	
}
