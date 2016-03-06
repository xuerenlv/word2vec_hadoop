package com.xhj_work;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class job1_reducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if (key.toString().length() == 0)
			return;

		long count = 0;
		for (Text text : values) {
			String val = text.toString();
			if (!val.contains("B")) {
				count += new Integer(val).intValue();
				continue;
			}
			context.write(key, text);
		}
		context.write(key, new Text(new Long(count).toString()));
	}

}
