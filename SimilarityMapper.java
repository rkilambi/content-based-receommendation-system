import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable,Text,Text,Text> {

	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {

		StringTokenizer tokens = new StringTokenizer(value.toString());
		String word = tokens.nextToken();			
		String[] words2 = tokens.nextToken().split(",");
		String[] words3 = tokens.nextToken().split(",");
		
		ArrayList<String> docs = new ArrayList<String>(Arrays.asList(words2));
		ArrayList<String> weights = new ArrayList<String>(Arrays.asList(words3));
		if(docs.size()!=1){
			for(int i = 0;i< docs.size()-1;i++){
				for(int j=i+1;j<docs.size();j++){
					String keystring = docs.get(i)+","+docs.get(j);
					String valuestring = weights.get(i)+","+weights.get(j);
					//DocPairs dp = new DocPairs(docs.get(i),docs.get(j));
					con.write(new Text(keystring),new Text(valuestring));
				}		
			}
		}
	}

}

