package com.show;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class InvertedIndexer {
	/** 自定义FileInputFormat **/
	public static class FileNameInputFormat extends FileInputFormat<Text, Text> {
		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileNameRecordReader fnrr = new FileNameRecordReader();
			fnrr.initialize(split, context);
			return fnrr;
		}
	}

	/** 自定义RecordReader **/
	public static class FileNameRecordReader extends RecordReader<Text, Text> {
		String fileName;
		LineRecordReader lrr = new LineRecordReader();

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return new Text(fileName);
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return lrr.getCurrentValue();
		}

		@Override
		public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
			lrr.initialize(arg0, arg1);
			fileName = ((FileSplit) arg0).getPath().getName();
		}

		public void close() throws IOException {
			lrr.close();
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {
			return lrr.nextKeyValue();
		}

		public float getProgress() throws IOException, InterruptedException {
			return lrr.getProgress();
		}
	}

	// reduce类的实现
	public static class InvertedIndexMapper extends Mapper<Text, Text, Text, IntWritable> {
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String temp = new String();// 临时变量，用于在后面存储word
			String line = value.toString().toLowerCase(); // 将value由text类型转为string类型，并转为小写
			StringTokenizer itr = new StringTokenizer(line); // 构造一个按空格分割的迭代器
			while (itr.hasMoreTokens()) {// 当还有单词时返回true，否则返回false
				temp = itr.nextToken(); // 一个单词
				Text word = new Text(); // 初始化key
				word.set(temp + "#" + key);// 对key设值
				context.write(word, new IntWritable(1));// 向reduce传送，value为1
			}
		}
	}

	/** 使用Combiner将Mapper的输出结果中value部分的词频进行统计 **/
	public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/** 自定义HashPartitioner，保证 <term, docid>格式的key值按照term分发给Reducer **/
	public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String term = new String();
			term = key.toString().split("#")[0]; // <term#docid>=>term
			return super.getPartition(new Text(term), value, numReduceTasks);
		}
	}

	// Reduce的实现
	public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
		private Text word1 = new Text(); // 当前处理的单词
		private Text word2 = new Text(); // 临时的结果
		String temp = new String();
		static Text CurrentItem = new Text(" "); // 当前处理的单词
		// 当前单词出现的 <文件名，出现次数> 串
		static List<String> postingList = new ArrayList<String>();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;// 出现的总数
			word1.set(key.toString().split("#")[0]); // 新传送过来的 单词
			temp = key.toString().split("#")[1]; // 新传送过来的 文件名
			for (IntWritable val : values) {// 对于同一个单词，将其出现次数累加
				sum += val.get();
			}
			word2.set("<" + temp + "," + sum + ">"); // 构造一个结果串
			// 当现在要处理的单词为空，或者跟上一个处理的单词不一样
			// 说明上一个单词被处理完了，要写入结果进行输出
			if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
				StringBuilder out = new StringBuilder();
				long sum_all = 0; // 出现的总数
				int count_doc = 0;// 文档的个数
				for (String p : postingList) {
					count_doc++;
					sum_all += Long.parseLong(p.substring(p.indexOf(",") + 1, p.indexOf(">")));
				}
				if (sum_all > 0) {
					DecimalFormat df = new DecimalFormat(".###"); // 对平均次数进行format
					out.append(df.format((double) sum_all / (double) count_doc));
					out.append(",");
				}
				for (String p : postingList) {
					String[] str = p.substring(1, p.length() - 1).split(",");
					if (str[0].length() > 14) {
						// 安装要求的格式输出
						out.append(str[0].substring(0, str[0].length() - 14) + ":" + str[1]);
						out.append(";");
					}
				}
				// 当当前结果不为空的时候，输出到文件中
				if (sum_all > 0)
					context.write(CurrentItem, new Text(out.toString()));
				// 初始化 postingList，其存储的结果被清空
				postingList = new ArrayList<String>();
			}
			CurrentItem = new Text(word1); // 设置当前处理的单词
			postingList.add(word2.toString()); // 不断向postingList也就是文档名称中添加词表
		}

		// cleanup 一般情况默认为空，有了cleanup不会遗漏最后一个单词的情况
		public void cleanup(Context context) throws IOException, InterruptedException {
			StringBuilder out = new StringBuilder();
			long sum_all = 0;
			// 文档的个数
			int count_doc = 0;
			for (String p : postingList) {
				count_doc++;
				sum_all += Long.parseLong(p.substring(p.indexOf(",") + 1, p.indexOf(">")));
			}

			if (sum_all > 0) {
				DecimalFormat df = new DecimalFormat(".###");
				out.append(df.format((double) sum_all / (double) count_doc));
				out.append(",");
			}
			for (String p : postingList) {
				String[] str = p.substring(1, p.length() - 1).split(",");
				if (str[0].length() > 14) {
					out.append(str[0].substring(0, str[0].length() - 14) + ":" + str[1]);
					out.append(";");
				}
			}
			if (sum_all > 0)
				context.write(CurrentItem, new Text(out.toString()));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// DistributedCache.addCacheFile(new
		// URI("hdfs://master01:54310/user/2014st08/stop-words.txt"), conf);//
		// 设置停词列表文档作为当前作业的缓存文件

		Job job = new Job(conf, "xhj new inverted index");
		job.setJarByClass(InvertedIndexer.class);
		job.setInputFormatClass(FileNameInputFormat.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(SumCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setPartitionerClass(NewPartitioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
