import java.util.*;
import java.lang.StringBuilder;
import java.io.*;
import java.lang.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

public class ljob{

	public static void main(String[] args) throws Exception{

		Configuration conf1 = new Configuration();
		Path path6 = new Path("/user/rkilambi/bigdata/output/path6");
		Path path4 = new Path("/user/rkilambi/bigdata/output/path4");
				
		FileSystem fs = FileSystem.get(conf1);
		
		conf1.set("fname",args[0]);
		
		if(fs.exists(path6))
			fs.delete(path6,true);

		Job job6 = new Job(conf1,"Recommended Mapper");
		job6.setJarByClass(RecoMapper.class);
		job6.setMapperClass(RecoMapper.class);
		job6.setMapOutputKeyClass(Text.class);
		job6.setMapOutputValueClass(Text.class);
		job6.setReducerClass(RecoReducer.class);
		job6.setNumReduceTasks(1);
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job6,path4);
		FileOutputFormat.setOutputPath(job6,path6);
		job6.setInputFormatClass(TextInputFormat.class);
		job6.setOutputFormatClass(TextOutputFormat.class);
		job6.waitForCompletion(true);

	}
}

