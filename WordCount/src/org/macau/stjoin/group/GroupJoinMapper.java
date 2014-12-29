package org.macau.stjoin.group;

/**
 * The Mapper uses the temporal information
 * R send same number 
 * S send one more time interval 
 * 
 */
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.macau.flickr.util.FlickrSimilarityUtil;
import org.macau.flickr.util.FlickrValue;
import org.macau.util.SimilarityUtil;

public class GroupJoinMapper extends
	Mapper<Object, Text, Text, IntWritable>{
	
	private final Text outputKey = new Text();
	
	private final FlickrValue outputValue = new FlickrValue();
	
	protected void setup(Context context) throws IOException, InterruptedException {

		System.out.println("Temporal mapper Start at " + System.currentTimeMillis());
	}
	
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
				
		//get the the file name which is used for separating the different set
		String fileName = ((FileSplit)inputSplit).getPath().getName();
				
		
		int tag = FlickrSimilarityUtil.getTagByFileName(fileName);
		
		long id =Long.parseLong(value.toString().split(":")[0]);
		double lat = Double.parseDouble(value.toString().split(":")[2]);
		double lon = Double.parseDouble(value.toString().split(":")[3]);
		long timestamp = Long.parseLong(value.toString().split(":")[4]);
		
		
		long timeInterval = timestamp / FlickrSimilarityUtil.TEMPORAL_THRESHOLD;
		
		
		String preTextual = "";
		String textual = value.toString().split(":")[5];
		
		if(!textual.equals("null")){
			
			String[] textualList = textual.split(";");
			
			
			//get the prefix values
			int prefixLength = SimilarityUtil.getPrefixLength(textualList.length, FlickrSimilarityUtil.TEXTUAL_THRESHOLD);
			
			for(int i = 0; i < prefixLength;i++){
				
				Integer tokenID = Integer.parseInt(textualList[i]);
				
				outputValue.setTileNumber(tokenID);
				
				preTextual += tokenID + "  ";
				
			}
			
		}
		
		double thres = Math.pow(FlickrSimilarityUtil.DISTANCE_THRESHOLD, 0.5);
		
		int x = (int) (lat /thres);
		int y = (int)(lon/thres );
		
		
//		outputValue = new FlickrValue(FlickrSimilarityUtil.getFlickrVallueFromString(value.toString()));
		
		outputValue.setTileNumber((int)timeInterval);
		
		outputValue.setId(id);
		outputValue.setLat(lat);
		outputValue.setLon(lon);
		outputValue.setTag(tag);
		
		//the textual information
		outputValue.setTiles(value.toString().split(":")[5]);
		
//		System.out.println(value.toString().split(":")[5]);
		
		outputValue.setTimestamp(timestamp);
		
		
		
		
		
		outputKey.set(" " + timeInterval + "  " + x + " " + y + "  "+ preTextual);
		context.write(outputKey, new IntWritable(1));
		
		
//		if(tag == FlickrSimilarityUtil.S_tag && timeInterval % 10 == 0){
//			
//			outputKey.set(timeInterval/10 - 1);
//			context.write(outputKey, outputValue);
//			
//		}
//		
//		outputKey.set(timeInterval/10);
//		context.write(outputKey, outputValue);
//		
		//The Original temporal partition, for each time interval, it is a partition, for the R
		//the time interval is the key, while for the S set, it should set to three time interval
		
		
		
		
//		if(tag == FlickrSimilarityUtil.S_tag){
//			
//			for(int i = -1; i<=1;i++){
//				outputKey.set(""+timeInterval + i);
//				outputValue.setTileNumber((int)timeInterval + i);
//				context.write(outputKey, outputValue);
//			}
//			
//		}else{
//			
//			//for the R set
//			outputKey.set(""+timeInterval);
//			context.write(outputKey, outputValue);
//		}
		
	
		
		
		
		
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		System.out.println("The Temporal mapper end at " + System.currentTimeMillis() + "\n");
	}
}