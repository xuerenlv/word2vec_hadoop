package com.twitter2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Calc_Triangle_Reducer extends Reducer<Text, Text, Text, Text> {
	private static long triangle_num = 0;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean exist = false;
		int count = 0;

		for (Text valuse : values) {
			if (valuse.toString().equalsIgnoreCase("#"))
				exist = true;
			else if (valuse.toString().equalsIgnoreCase("&"))
				count++;
			else
				System.out.println("wrong");
		}

		if (exist)
			triangle_num += count;
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("trangle num:"), new Text("#" + new Long(triangle_num).toString()));
	}
}
