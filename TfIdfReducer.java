import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.lang.StringBuilder;
import java.util.*;
import java.math.*;


public class TfIdfReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key,Iterable<Text> values,Context con)throws IOException,InterruptedException{

		Configuration conf = con.getConfiguration();
		Integer docNum = conf.getInt("docNum",1000);
		Integer n,N;
		double tfidf;
		String documents;

		Map<String,Float> map = new TreeMap<String,Float>();
		
		int count = 0;

		Iterator<Text> ite = values.iterator();
		while(ite.hasNext()){
			String rest = ite.next().toString();
			
			StringTokenizer words = new StringTokenizer(rest,",");
						
			documents = words.nextToken();
			//term weight
			n = Integer.parseInt(words.nextToken());
			N = Integer.parseInt(words.nextToken());
			float var = (float)n/N;
			var = 0.5f + 0.5f * var;			
			map.put(documents,var);
			count++;			
			
		}
		//document weight
		int ndocs = map.size();
		double res = Math.log((float)docNum/ndocs);
				
		//for constructing a csv document list on a single line
		Set<String> set = map.keySet();	

		StringBuilder docsv = new StringBuilder(" ");
		Iterator<String> docs = set.iterator();
		docsv.append(docs.next());
		while(docs.hasNext()){
			docsv.append(","+docs.next());
		}
		
		StringBuilder tfidfcsv = new StringBuilder("");
				
		//iterator for looping through division of n and N
		LinkedList<Float> list = new LinkedList<Float>(map.values());
		Iterator<Float> it = list.iterator();
		while(it.hasNext()){
			float val = it.next();
			tfidf = (val) * (res);
			tfidfcsv.append(tfidf+",");
		}
		tfidfcsv.deleteCharAt(tfidfcsv.length()-1);
		String result = docsv +" "+tfidfcsv;
		con.write(key,new Text(result));
			
		}
		

	
}
