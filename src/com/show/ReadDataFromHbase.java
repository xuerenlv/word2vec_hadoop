package com.show;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class ReadDataFromHbase {

	public static void main(String[] args) throws IOException, Exception {

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum.", "localhost"); // 千万别忘记配置
		HTable table = new HTable(config, "wuxia");

		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);

		File file = new File("./src/result.txt");
		if (!file.exists())
			file.createNewFile();
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file));

		for (Result result : resultScanner) {
			for (KeyValue kv : result.raw()) {
				if (!new String(kv.getRow()).equals(new String(kv.getValue()))) {
					String valu = new String(kv.getValue());
					bufferedWriter.write(new String(kv.getRow()) + "\t" + valu.substring(0, valu.indexOf(",")) + "\n");
					// System.out.println(new String(kv.getRow()) + "->" +
					// valu.substring(0, valu.indexOf(",")));
				}
			}
		}
	}
}