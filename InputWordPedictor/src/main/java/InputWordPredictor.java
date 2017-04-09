
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.json.JSONArray;
import org.json.JSONObject;
/**
 * Created by flora on 2017/4/8.
 */
public class InputWordPredictor {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

		private Text keyWord = new Text();
		private Text valueWord = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split("\t")[0].split(" ");
			if (input.length > 1) {
				String keyOutput = "";
				for (int i = 0; i < input.length - 1; i++) {
					keyOutput += input[i] + " ";
				}
				keyOutput = keyOutput.substring(0, keyOutput.length() - 1);
				valueWord.set(input[input.length - 1] + "#" + value.toString().split("\t")[1]);
				keyWord.set(keyOutput);
				context.write(keyWord, valueWord);
			}

		}
	}

	public static class IntSumReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			Map<String, Double> proMap = new HashMap<String, Double>();
			List<Map.Entry<String, Double>> sortedProMap = null;

			List<String> buffer = new ArrayList<String>();
			for (Text value : values) {
				buffer.add(value.toString());
			}

			for (String val : buffer) {
				String[] temp = val.split("#");
				if (temp.length == 2) {
					// String str1 = valvalue.substring(0,
					// valvalue.indexOf("#"));
					// String str2 = valvalue.substring(valvalue.indexOf("#") +
					// 1);
					proMap.put(temp[0], Double.parseDouble(temp[1]));
					sum += Double.parseDouble(temp[1]);
				}
			}

			for (String mapKey : proMap.keySet()) {
				proMap.put(mapKey, proMap.get(mapKey) / sum);
			}
			sortedProMap = new ArrayList<HashMap.Entry<String, Double>>(proMap.entrySet());
			sortedProMap.sort((x, y) -> {
				double diff = y.getValue() - x.getValue();
				return diff > 0 ? 1 : (diff < 0 ? -1 : 0);
			});
			//JSONArray array = new JSONArray();
			//JSONObject jObject = null;

			Put put = new Put(key.getBytes());
			int N = Integer.parseInt(context.getConfiguration().get("nvalue"));
			int realNum = N < sortedProMap.size() ? N : sortedProMap.size();
			if (realNum > 0) {
				for (Map.Entry<String, Double> entry : sortedProMap.subList(0, realNum)) {
					//jObject = new JSONObject();
					//jObject.put(entry.getKey(), entry.getValue());
					//array.put(jObject);

					put.addColumn(Bytes.toBytes("data"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
				}

				context.write(new ImmutableBytesWritable(key.getBytes()), put);
			}
			// valueWord.set(array.toString());
			// context.write(key, valueWord);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String zkAddr = "ip-172-31-1-110.ec2.internal";
		conf.set("hbase.zookeeper.quorum", zkAddr);
		//HBaseAdmin admin = new HBaseAdmin(conf);
		//if (admin.tableExists("InputWordPredictor")) {
		//	admin.disableTable("InputWordPredictor");
		//	admin.deleteTable("InputWordPredictor");
		//}
		conf.set("nvalue", args[1]);
		Job job = new Job(conf, "predictor");
		//HTableDescriptor hTableDescriptor = new HTableDescriptor("InputWordPredictor");
		//HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("data");
		//hTableDescriptor.addFamily(hColumnDescriptor);
		//admin.createTable(hTableDescriptor);
		job.setJarByClass(InputWordPredictor.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		// job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass((Text.class));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		TableMapReduceUtil.initTableReducerJob("InputWordPredictor", IntSumReducer.class, job);
		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.addDependencyJars(job.getConfiguration());
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
