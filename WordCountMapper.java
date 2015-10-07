import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable,Text,WordDoc,IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	private static Set<String> Stopwords;
 
    static {
        Stopwords = new HashSet<String>();
        Stopwords.add("I"); Stopwords.add("a");
        Stopwords.add("about"); Stopwords.add("an");
        Stopwords.add("are"); Stopwords.add("as");
        Stopwords.add("at"); Stopwords.add("be");
        Stopwords.add("by"); Stopwords.add("com");
        Stopwords.add("de"); Stopwords.add("en");
        Stopwords.add("for"); Stopwords.add("from");
        Stopwords.add("how"); Stopwords.add("in");
        Stopwords.add("is"); Stopwords.add("it");
        Stopwords.add("la"); Stopwords.add("of");
        Stopwords.add("on"); Stopwords.add("or");
        Stopwords.add("that"); Stopwords.add("the");
        Stopwords.add("this"); Stopwords.add("to");
        Stopwords.add("was"); Stopwords.add("what");
        Stopwords.add("when"); Stopwords.add("where");
        Stopwords.add("who"); Stopwords.add("will");
        Stopwords.add("with"); Stopwords.add("and");
        Stopwords.add("the"); Stopwords.add("www");
	Stopwords.add("its"); Stopwords.add("am"); 
    }


	public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException {

		String fileName = ((FileSplit) con.getInputSplit()).getPath().getName();

		String words = value.toString().replaceAll("[^a-zA-Z\\s]"," ").replaceAll("\\s+"," ").toLowerCase();
		StringTokenizer tokens = new StringTokenizer(words);
		while(tokens.hasMoreTokens()){
			String next = tokens.nextToken();
			if(!Stopwords.contains(next))
				word.set(next);
			else
				continue;
			WordDoc wd = new WordDoc(new Text(fileName),word);
			con.write(wd,one);
		}

	} 


}
