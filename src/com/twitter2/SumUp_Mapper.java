package com.twitter2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumUp_Mapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String num_count = value.toString().substring(value.toString().indexOf("#")+1);
		context.write(new Text("count"), new Text(num_count));
	}
}
