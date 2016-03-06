package com.xhj_work;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ContralClass {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		// *************************************************** job1 start
		Job job1 = new Job(conf, "job1 undirected graph");
		job1.setJarByClass(ContralClass.class);
		job1.setMapperClass(job1_mapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setReducerClass(job1_reducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

		// Path word_map_hufcode = new Path("job1_temp_word_map_hufcode");
		// Path hufcode_map_vec = new Path("job1_temp_hufcode_map_vec");
		// *************************************************** job1 end

		// distributed job1 result
		DistributedCache.addCacheFile(new Path(args[1]+"/part-r-00000").toUri(), conf);

		// *************************************************** job2 start
		Job job2 = new Job(conf, "job2 training phrase");
		job2.setJarByClass(ContralClass.class);
		job2.setMapperClass(job2_mapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(job2_reducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(job1.isComplete());

		// *************************************************** job2 end

		// FileSystem.get(conf).deleteOnExit(temp_dir_1);
		System.exit(1);
	}

}
