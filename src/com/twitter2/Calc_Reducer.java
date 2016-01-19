package com.twitter2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Calc_Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> exist_edge = new ArrayList<String>();

		for (Text value : values) {
			exist_edge.add(value.toString());
			context.write(new Text(key.toString() + "->" + value.toString()), new Text("#"));
		}

		for (int i = 0; i < exist_edge.size() - 1; i++) {
			for (int j = i + 1; j < exist_edge.size(); j++) {
				if (Long.valueOf(exist_edge.get(i)) < Long.valueOf(exist_edge.get(j))) {
					context.write(new Text(exist_edge.get(i).toString() + "->" + exist_edge.get(j).toString()),
							new Text("&"));
				} else {
					context.write(new Text(exist_edge.get(j).toString() + "->" + exist_edge.get(i).toString()),
							new Text("&"));
				}
			}
		}
	}

}
