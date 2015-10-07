import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TermNumberMapper extends Mapper<LongWritable,Text,Text,Text> {

	private Text word = new Text();

	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {

		StringTokenizer tokens = new StringTokenizer(value.toString(),",");

		
		String document = tokens.nextToken();
		String rest = tokens.nextToken();
		
		con.write(new Text(document),new Text(rest));
	}

} 







