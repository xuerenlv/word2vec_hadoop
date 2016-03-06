package com.xhj_work;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class job1_mapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");

		for (int i = 0; i < words.length; i++) {
			context.write(new Text(words[i]), new Text("1"));

			String word_context = "";
			for (int j = i - 3; j <= i + 3; j++) {
				if (j < 0 || j == i || j >= words.length)
					continue;
				word_context += " " + words[j];
			}
			context.write(new Text(words[i]), new Text(" B " + word_context + " B "));
		}
	}

}
