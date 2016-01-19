package com.twitter2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Calc_Mapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] start_end = value.toString().trim().split("->");
		context.write(new Text(start_end[0]), new Text(start_end[1]));
	}
}
