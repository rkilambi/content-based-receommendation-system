import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.lang.StringBuilder;
import java.lang.Math;
import java.util.*;
import java.math.*;


public class SimilarityReducer extends Reducer<Text,Text,Text,Text> {
	
	public void reduce(Text key,Iterable<Text> values,Context con)throws IOException,InterruptedException{

		LinkedList<Float> doc_A = new LinkedList<Float>();
		LinkedList<Float> doc_B = new LinkedList<Float>();
		

		Iterator<Text> ite = values.iterator();
		while(ite.hasNext()){
			String rest = ite.next().toString();
			
			String[] val = new String(rest).split(",");
			doc_A.add(Float.parseFloat(val[0]));
			doc_B.add(Float.parseFloat(val[1]));
			
			
		}
		
		//calculating the cosine similarity
		Iterator<Float> A = doc_A.iterator();
		Iterator<Float> B = doc_B.iterator();
		float sum = 0,sums=0;
		while(A.hasNext() && B.hasNext()){
			float x,y;
			x= A.next();y=B.next();
			sum+=(x*y);
			sums=sum+(x*x)+(y*y);
		}	
		
		float den =(float) Math.sqrt(sums);
		float result = (float)(sum/den);	
		
		con.write(key,new Text(Float.toString(result)));

		
	}
		

	
}
