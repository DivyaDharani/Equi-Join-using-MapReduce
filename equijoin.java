import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {
	
	public static void main(String args[]) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "equijoin");
		job.setJarByClass(equijoin.class);
		job.setMapperClass(EquiJoinMapper.class);
		job.setReducerClass(EquiJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path inputPath = new Path(args[1]); 
		Path outputPath = new Path(args[2]);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
	
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class EquiJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String line = value.toString();
				String[] contents = line.split(",");
				/*Example:
				R, 2, Don, Larson, Newark, 555-3221
				S, 1, 33000, 10000, part1
				*/
				
				Text outputKey = null; 
				if(contents.length >= 2) {
					outputKey = new Text(contents[1].trim()); //table join column value is the key - sid
				}
				
				context.write(outputKey, value);
			}
			catch(Exception e) {
				
			}
		}
	}
	
	public static class EquiJoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			try {
				HashMap<String, List<String>> dict = new HashMap<String, List<String>>();
				ArrayList<String> tableNames = new ArrayList<String>();
				
				String tableName = null;
				for(Text value : values) {
					String line = value.toString();
					tableName = line.split(",")[0].toUpperCase().trim(); //to extract the table name
					if(!dict.containsKey(tableName)) {
						dict.put(tableName, new ArrayList<String>());
					}
					dict.get(tableName).add(line);
				}
				
				List<List<String>> tableValues = new ArrayList<List<String>>();
				for(String k : dict.keySet()) {
					tableValues.add(dict.get(k));
				}
				
				if(tableValues.size() >= 2) { //2 tables
					for(String table1Row : tableValues.get(0)) {
						for(String table2Row : tableValues.get(1)) {
							context.write(new Text(table1Row.trim() + ", " + table2Row.trim()), new Text(""));
						}
					}
				}
			}
			catch(Exception e) {
				
			}
		}
	}
}


