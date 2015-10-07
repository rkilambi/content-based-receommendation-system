import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.lang.StringBuilder;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;

public class RecoReducer extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key,Iterable<Text> values,Context con)throws IOException,InterruptedException{
		int count = 0;
		Configuration conf = con.getConfiguration();
		String fnam = conf.get("fname");
		Iterator<Text> ite = values.iterator();
		if((key.toString()).equals(fnam)){
			while(ite.hasNext()){
				count++;
				con.write(new Text(fnam),ite.next());
				if(count==30)break;
			}		
		}
	}
}
