import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class WordDoc implements WritableComparable<WordDoc>{

	private Text word;
	private Text document;

	@Override
	public int compareTo(WordDoc wd) {
		if(word.compareTo(wd.word) == 0) return document.compareTo(wd.document);
		else return word.compareTo(wd.word);

	}

	public WordDoc() {
		word = new Text();
		document = new Text();
		// TODO Auto-generated constructor stub
	}

	public WordDoc(Text Docid,Text term) {
		word = term;
		document = Docid;
		// TODO Auto-generated constructor stub
	}


	@Override
	public void write(DataOutput out) throws IOException{
		// TODO Auto-generated method stub
		word.write(out);
		document.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		// TODO Auto-generated method stub
	word.readFields(in);
	document.readFields(in);
	}
	@Override
 	public String toString() {
 	return document+","+word;

	}
	@Override
 	public int hashCode() {
 	return document.hashCode();
	}
	public Text getDoc(){
	return this.document;
	}
	public Text getWord(){
	return this.word;
	}

}
