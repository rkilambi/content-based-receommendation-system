import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StringFinderMapper extends Mapper<LongWritable,Text,Text,Text> {
	
	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {

		Configuration conf = con.getConfiguration();
		String uquery = conf.get("Query");
		String[] qvalues = uquery.split(" ");
		Set<String> uqset = new HashSet<String>(Arrays.asList(qvalues));
	
					

		StringTokenizer tokens = new StringTokenizer(value.toString());
		String term = tokens.nextToken();
		if(uqset.contains(term)){
			StringTokenizer words2 = new StringTokenizer(tokens.nextToken(),",");
			StringTokenizer words3 = new StringTokenizer(tokens.nextToken(),",");

			//TreeMap<Float,String> mapping = new TreeMap<Float,String>();

			while(words2.hasMoreTokens() && words3.hasMoreTokens()){
				//mapping.put(Float.parseFloat(words3.nextToken()),words2.nextToken());
				con.write(new Text(words2.nextToken()),new Text(words3.nextToken()));
			}
			//String values1 = Float.toString(mapping.firstEntry().getKey());
//			con.write(new Text(mapping.firstEntry().getValue()),new Text (values1));
		}
	}
}



