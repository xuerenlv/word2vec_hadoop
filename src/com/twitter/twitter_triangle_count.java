package com.twitter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class twitter_triangle_count {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job1 = new Job(conf, "job1——undirected graph");
		job1.setJarByClass(twitter_triangle_count.class);
		job1.setMapperClass(UndirectedMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setReducerClass(UndirectedReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setNumReduceTasks(10);

		// job1 的文件输入目录
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		// job1 的文件输出目录
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "job2--calculate triangle number");
		job2.setJarByClass(twitter_triangle_count.class);
		job2.setMapperClass(Calc_Mapper.class);
		job2.setCombinerClass(Calc_Combiner.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(Calc_Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(10);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job2.waitForCompletion(job1.isComplete());

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}

}
