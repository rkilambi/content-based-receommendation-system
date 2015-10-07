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

public class TfIdfmain{

	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		Path ip = new Path(args[0]);
		Path op = new Path(args[1]);
		Path path1 = new Path(op +"/path1");
		Path path2 = new Path(op+"/path2");

		Path path3 = new Path(op+"/path3");

		Path path4 = new Path(op+"/path4");

		Path path5 = new Path(op+"/path5");

		
		FileSystem fs = FileSystem.get(conf);
		ContentSummary cs = fs.getContentSummary(ip);
		int fileCount =(int) cs.getFileCount();

		if(fs.exists(path1))
			fs.delete(path1,true);
		if(fs.exists(path2))
			fs.delete(path2,true);
		if(fs.exists(path3))
			fs.delete(path3,true);
		if(fs.exists(path4))
			fs.delete(path4,true);
		if(fs.exists(op))
			fs.delete(op,true);

		
		Job job1 = new Job(conf,"Tfidf");
		job1.setJarByClass(TfIdfmain.class);
		FileInputFormat.addInputPath(job1,ip);
		FileOutputFormat.setOutputPath(job1,path1 );
		job1.setMapOutputKeyClass(WordDoc.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setMapperClass(WordCountMapper.class);
		job1.setReducerClass(WordCountReducer.class);
		job1.setNumReduceTasks(fileCount);
		job1.waitForCompletion(true);

		Job job2 = new Job(conf,"Term Counter");
		job2.setJarByClass(TfIdfmain.class);
		job2.setMapperClass(TermNumberMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(TermNumberReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2,path1);
		FileOutputFormat.setOutputPath(job2,path2);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setNumReduceTasks(fileCount);
		job2.waitForCompletion(true);

		conf.setInt("docNum",fileCount);
		Job job3 = new Job(conf,"Tfidfcalculation");
		job3.setJarByClass(TfIdfmain.class);
		job3.setMapperClass(TfIdfMapper.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setReducerClass(TfIdfReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3,path2);
		FileOutputFormat.setOutputPath(job3,path3);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		job3.setNumReduceTasks(fileCount);
		job3.waitForCompletion(true);

		Job job4 = new Job(conf,"Similarity Matrix");
		job4.setJarByClass(SimilarityMapper.class);
		job4.setMapperClass(SimilarityMapper.class);
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setReducerClass(SimilarityReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4,path3);
		FileOutputFormat.setOutputPath(job4,path4);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		job4.setNumReduceTasks(fileCount);
		job4.waitForCompletion(true);

		System.out.println("Enter the user String to query:");
		Scanner in = new Scanner(System.in);
		String query = in.nextLine();
					
		if(fs.exists(path5))
			fs.delete(path5,true);
	
		Configuration conf1 = new Configuration();
		conf1.set("Query",query);
		Job job5 = new Job(conf1,"String finder");
		job5.setJarByClass(StringFinderMapper.class);
		job5.setMapperClass(StringFinderMapper.class);
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(Text.class);
		job5.setReducerClass(StringFinderReducer.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job5,path3);
		FileOutputFormat.setOutputPath(job5,path5);
		job5.setInputFormatClass(TextInputFormat.class);
		job5.setOutputFormatClass(TextOutputFormat.class);
		job5.setNumReduceTasks(1);
		job5.waitForCompletion(true);

	}
}
