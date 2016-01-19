package com.twitter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Calc_Reducer extends Reducer<Text, Text, Text, Text> {
	// 纪录三角形的个数
	static long triangle_num = 0;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean exist = false;
		int houxuan = 0;

		for (Text value : values) {
			if (value.toString().equals("already_exist"))
				exist = true;
			if (value.toString().equals("does_exist"))
				houxuan++;
			if (value.toString().indexOf('#') != -1) {
				houxuan += Integer.valueOf(value.toString().substring(1));
			}
		}

		if (exist)
			triangle_num += houxuan;
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("trangle num:"), new Text(new Long(triangle_num).toString()));
	}
}
