package com.twitter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Calc_Combiner extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean already_exist = false;
		int hou_xuan = 0;
		for (Text one_text : values) {
			if (one_text.toString().equals("already_exist"))
				already_exist = true;
			if (one_text.toString().equals("does_exist"))
				hou_xuan++;
		}
		if (hou_xuan != 0)
			context.write(key, new Text("#" + new Integer(hou_xuan).toString()));
		if (already_exist)
			context.write(key, new Text("already_exist"));
	}
}
