import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;


public class WordCountReducer extends Reducer<WordDoc,IntWritable,WordDoc,IntWritable> {
	
	public void reduce(WordDoc key,Iterable<IntWritable> values,Context con)throws IOException,InterruptedException{
		
		int sum = 0;
		for(IntWritable loop:values){
			sum+=loop.get();			
		}
		con.write(key,new IntWritable(sum));
		
	}

}
