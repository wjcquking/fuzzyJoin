package org.macau.flickr.knn.exact.second;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.knn.exact.first.kNNPartition;
import org.macau.flickr.knn.util.kNNUtil;
import org.macau.flickr.util.FlickrPartitionValue;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.spatial.Distance;

public class kNNJoinMapper extends
	Mapper<Object,Text,IntWritable,FlickrPartitionValue>{
	
	private final IntWritable outputKey = new IntWritable();
	private final FlickrPartitionValue outputValue = new FlickrPartitionValue();
	public static List<FlickrValue> pivotList = new ArrayList<FlickrValue>();
	public static double[][] LBArray = new double[kNNUtil.REDUCER_NUMBER][kNNUtil.REDUCER_NUMBER];
	

	
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
	
	/**
	 * compute the Lower bound for each Partition of S
	 */
	public static void compuateLBOfReplica(kNNPartition[] R_Partition,kNNPartition[] S_Partition,List<Double> thetaList){
		
		for(int j = 0; j < kNNUtil.REDUCER_NUMBER;j++){
			for(int i = 0;i < kNNUtil.REDUCER_NUMBER;i++){
				
				double distance = Distance.GreatCircleDistance(R_Partition[i].getLat(),  R_Partition[i].getLon(),  S_Partition[j].getLat(), S_Partition[j].getLon());
				
				LBArray[j][i]= distance - R_Partition[i].getMaxDistance()-thetaList.get(i);
						
			}
		}
	}
	/**
	 * the setup function complete LB of Replicas
	 */
	protected void setup(Context context) throws IOException, InterruptedException {
		
		System.out.println("setup");
		// compute the Lower Bound of replication
		
		
		
		
		/*
		String path = kNNUtil.pivotOutputPath + "/flickr.kmeans.data";
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
		
		FileSystem hdfs = FileSystem.get(conf);
		
		String value = null;
		Path pathq = new Path(path);
		FSDataInputStream fsr = hdfs.open(pathq);
		BufferedReader bis = new BufferedReader(new InputStreamReader(fsr,"UTF-8")); 

		
	    while ((value = bis.readLine()) != null) {
	    	//System.out.println(value);
	    	FlickrValue fv = new FlickrValue();
			long id =Long.parseLong(value.toString().split(";")[0]);
			double lat = Double.parseDouble(value.toString().split(";")[1]);
			double lon = Double.parseDouble(value.toString().split(";")[2]);
			long timestamp = Long.parseLong(value.toString().split(";")[3]);
			

			
			fv.setId(id);
			fv.setLat(lat);
			fv.setLon(lon);
			fv.setTimestamp(timestamp);
			
			pivotList.add(new FlickrValue(fv));
	    }

		*/
		
	}
	
	public void map(Object key,Text value,Context context)
		throws IOException, InterruptedException{
		
		FlickrPartitionValue fpv = new FlickrPartitionValue();
		int pid = Integer.parseInt(value.toString().split(";")[0]);
		
		//R: 0; S:1
		int dataset = Integer.parseInt(value.toString().split(";")[1]);
		double distance  = Double.parseDouble(value.toString().split(";")[2]);
		long id =Long.parseLong(value.toString().split(";")[3]);
		
		fpv.setPid(pid);
		fpv.setDataset(dataset);
		fpv.setDistance(distance);
		fpv.setId(id);
		
		
		outputValue.setPid(pid);
		outputValue.setDataset(dataset);
		outputValue.setDistance(distance);
		outputValue.setId(id);
		
		//R
		if(dataset == FlickrSimilarityUtil.R_tag){
			outputKey.set(pid);
			
			context.write(outputKey,outputValue);
			
		}else{
			
			for(int i = 0;i < kNNUtil.REDUCER_NUMBER;i++){
				if(LBArray[pid][i] < distance){
					outputKey.set(i);
					context.write(outputKey,outputValue);
				}
			}
		}
		
		

		
		
		context.write(outputKey,outputValue);
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("clean up");
		
		
	}
}
