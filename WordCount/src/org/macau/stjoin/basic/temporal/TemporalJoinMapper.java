package org.macau.stjoin.basic.temporal;

/**
 * The Mapper uses the temporal information
 * 
 */
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;

public class TemporalJoinMapper extends
	Mapper<Object, Text, LongWritable, FlickrValue>{
	
	private final LongWritable outputKey = new LongWritable();
	
	private final FlickrValue outputValue = new FlickrValue();
	
	public static String convertDateToString(Date date){
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
		return df.format(date);
	}
	
	public static Date convertLongToDate(Long date){
		return new Date(date);
	}
	
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		InputSplit inputSplit = context.getInputSplit();
		
		//R: 0; S:1
		int tag;
		
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		
		if(fileName.contains(FlickrSimilarityUtil.R_TAG)){
			
			tag = 0;
			
		}else{
			tag = 1;
		}
		
		long id =Long.parseLong(value.toString().split(":")[0]);
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		/* Convert the timestamp to the Date
		 * use the day as key
		 * the all value as a value
		 * use the timestamp to refine and compare the distance
		 */
		
//		long previousTimeStamp = timestamp - MS_OF_ONE_DAY;
//		long laterTimestamp = timestamp + MS_OF_ONE_DAY;
		
		long timeInterval = timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		
		
		outputValue.setTileNumber((int)timeInterval);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tag);
		
		//the textual information
		outputValue.setTiles(value.toString().split(":")[5]);
		
		outputValue.setTimestamp(timestamp);
		
//		System.out.println("map" + (timeInterval/10 + 1));
		
		if(timeInterval % 10 == 9){
			
			outputKey.set(timeInterval/10 + 1);
			context.write(outputKey, outputValue);
			
		}
		
		outputKey.set(timeInterval/10);
		context.write(outputKey, outputValue);
		
		
	}
}