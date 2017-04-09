
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Created by flora on 2017/4/8.
 */
public class NgramCount {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String text = value.toString();
			text = CleanString(GetRevision(text));
			String[] textSplit = text.split(" ");

			for (int i = 0; i < (textSplit.length); i++) {
				String temp = textSplit[i];
				for (int gram = 1; gram <= 4 && i + gram < textSplit.length; gram++) {
					word.set(temp);
					context.write(word, one);
					temp += " " + textSplit[i + gram];
				}
				word.set(temp);
				context.write(word, one);
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if(sum>2){
				result.set(sum);
				context.write(key, result);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(NgramCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static String GetRevision(String rawXML) {
		try {
			DocumentBuilder xmlBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document xmlDoc = xmlBuilder.parse(new ByteArrayInputStream(rawXML.getBytes("utf-8")));
			Element pageElement = xmlDoc.getDocumentElement();
			Element revisionElement = (Element) pageElement.getElementsByTagName("revision").item(0);
			return revisionElement.getElementsByTagName("text").item(0).getTextContent();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public static String CleanString(String rawString) {
		final String urlRegex = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
		final String refRegex = "<ref(\\s[^>]*?)?>|<\\/ref>";
		final String apostrophesRegex = "[^a-z]'|'[^a-z]";
		final String alphabeticRegex = "[^a-z']";
		final String continuousSpaceRegex = "\\s+";
		return rawString.toLowerCase().replaceAll(urlRegex, " ").replaceAll(refRegex, " ")
				.replaceAll(apostrophesRegex, " ").replaceAll(alphabeticRegex, " ")
				.replaceAll(continuousSpaceRegex, " ").trim();
	}
}
