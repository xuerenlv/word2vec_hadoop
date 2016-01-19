package com.twitter2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumUp_Reducer extends Reducer<Text, Text, Text, Text> {
	private static long all_triangle_num = 0;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			all_triangle_num += Long.valueOf(value.toString());
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("all trangle num:"), new Text(new Long(all_triangle_num).toString()));
	}
}
