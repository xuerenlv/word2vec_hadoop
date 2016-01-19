package com.twitter;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UndirectedReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String second_node_string = key.toString();
		// 对一个起点的，所到达的节点用 “＋” 连接起来，便于下一步的处理
		for (Text second_str : values) {
			second_node_string += "#" + second_str.toString();
		}
		context.write(new Text(second_node_string), new Text("#"));
	}
}
