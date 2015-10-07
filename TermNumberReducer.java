import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.util.Iterator;
import java.util.LinkedList;
import java.lang.StringBuilder;
import java.util.*;

public class TermNumberReducer extends Reducer<Text,Text,Text,Text> {
	private int sum = 0;
	public void reduce(Text key,Iterable<Text> values,Context con)throws IOException,InterruptedException{
		
		LinkedList<String> valueslist = new LinkedList<String>();
		LinkedList<Integer> valuesSum = new LinkedList<Integer>();
		Iterator<Text> ite = values.iterator();
		while(ite.hasNext()){
			String rest = ite.next().toString();
		
			StringTokenizer words = new StringTokenizer(rest);				
			valueslist.add(words.nextToken());
			valuesSum.add(Integer.parseInt(words.nextToken()));
			
		}
		Iterator<Integer> count = valuesSum.iterator(); 
		while(count.hasNext()){
			sum +=count.next(); 
		}	


		StringBuilder key1 = new StringBuilder(key.toString());
		key1.append(","+sum);
		key.set(key1.toString());
		
		Iterator<String> ite2 = valueslist.iterator();
		int loop = 0;
		while(ite2.hasNext()){
			String val = ite2.next() + ","+ valuesSum.get(loop);
			con.write(key,new Text(val));
			loop++;
		}
		
	}
}
