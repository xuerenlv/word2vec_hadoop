package com.twitter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Calc_Mapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] start_end = value.toString().replace(" ", "").replace("\t", "").split("#");

		// 把已存在的节点向 Reduce 发送
		String start = start_end[0];
		for (int i = 1; i < start_end.length; i++) {
			if (start_end[i].length() == 0)
				continue;
			context.write(new Text(start + "->" + start_end[i]), new Text("already_exist"));
		}
		// 如果存在，则存在一个三角形
		for (int i = 1; i < start_end.length - 1; i++) {
			for (int j = i + 1; j < start_end.length; j++) {
				if (start_end[i].length() == 0 || start_end[j].length() == 0)
					continue;
				
				if (Long.valueOf(start_end[i]) < Long.valueOf(start_end[j])) {
					context.write(new Text(start_end[i] + "->" + start_end[j]), new Text("does_exist"));
				} else {
					context.write(new Text(start_end[j] + "->" + start_end[i]), new Text("does_exist"));
				}
			}
		}
	}
}
