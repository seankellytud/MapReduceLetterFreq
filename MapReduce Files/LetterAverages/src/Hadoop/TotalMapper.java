package Hadoop;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TotalMapper extends Mapper<LongWritable, Text , TextPair, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//regular expression to allow letters from any language and discount numbers/special chars
		StringTokenizer tokenizer = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^\\p{L}]", ""));
		// Book files are named according to language and this is how the output language is extracted
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		//checks if filename contains the languages and passes it into language variable for output
		String language = fileName.replaceAll("[0-9]", "");
        while (tokenizer.hasMoreTokens())
        {

        	String str = tokenizer.nextToken();
            for(int i = 0; i<str.length(); i++)
            {
            	// equals sign will be used later for extracting its corresponding textpair value 
                context.write(new TextPair(language, "="), one);
            }
        }
	}

}
