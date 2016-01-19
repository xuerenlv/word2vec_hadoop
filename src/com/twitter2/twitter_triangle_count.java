package com.twitter2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.cliffc.high_scale_lib.NonBlockingIdentityHashMap;

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
		job1.setNumReduceTasks(1);

		// job1 的文件输入目录 文件输出目录
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "job2--sort edge");
		job2.setJarByClass(twitter_triangle_count.class);
		job2.setMapperClass(Calc_Mapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(Calc_Reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(job1.isComplete());

		Job job3 = new Job(conf, "job3--calculate triangle num");
		job3.setJarByClass(twitter_triangle_count.class);
		job3.setMapperClass(Calc_Triangle_Mapper.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setReducerClass(Calc_Triangle_Reducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		job3.waitForCompletion(job2.isComplete());

		Job job4 = new Job(conf, "job4--sum up all triangle num");
		job4.setJarByClass(twitter_triangle_count.class);
		job4.setMapperClass(SumUp_Mapper.class);
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setReducerClass(SumUp_Reducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job4, new Path(args[3]));
		FileOutputFormat.setOutputPath(job4, new Path(args[4]));
		job4.waitForCompletion(job3.isComplete());

		System.exit(job4.waitForCompletion(true) ? 0 : 1);
	}

}
