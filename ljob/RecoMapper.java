import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecoMapper extends Mapper<LongWritable,Text,Text,Text> {

	private Text word = new Text();

	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {

		StringTokenizer tokens = new StringTokenizer(value.toString());
				
		String[] words1 = tokens.nextToken().split(",");
		String[] words2 = tokens.nextToken().split(",");
		for(int i=0;i<2;i++){
			String keyval = words1[i%2];
			String val = words1[(i+1)%2];
			
			con.write(new Text(keyval),new Text(val));
		}		
	}

}

