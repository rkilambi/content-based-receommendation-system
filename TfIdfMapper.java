import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TfIdfMapper extends Mapper<LongWritable,Text,Text,Text> {

	private Text word = new Text();

	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {

		StringTokenizer tokens = new StringTokenizer(value.toString());
				
		String[] words1 = tokens.nextToken().split(",");
		String[] words2 = tokens.nextToken().split(",");
		
		String rest = words1[0]+","+words2[1]+","+words1[1];		
		
		con.write(new Text(words2[0]),new Text(rest));
	}

}

