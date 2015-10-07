import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.lang.StringBuilder;
import java.util.*;
import java.math.*;


public class StringFinderReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key,Iterable<Text> values,Context con)throws IOException,InterruptedException{
		
		double sum = 0;

		float var;
		Iterator<Text> it = values.iterator();
		
		while(it.hasNext()){
			var = Float.parseFloat(it.next().toString());
			sum = sum + var;
			
		}
		con.write(key,new Text(Float.toString((float)sum)));
							
		}
		

	
}
