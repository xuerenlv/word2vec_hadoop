package com.twitter2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UndirectedMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] two_node = value.toString().split(" ");
		// 取值小的点，作为起点
		if (Long.valueOf(two_node[0]) < Long.valueOf(two_node[1])) {
			context.write(new Text(two_node[0] + "->" + two_node[1]), new Text(""));
		} else {
			context.write(new Text(two_node[1] + "->" + two_node[0]), new Text(""));
		}
	}
}
