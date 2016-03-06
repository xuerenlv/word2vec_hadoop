package com.xhj_work;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.xhj_work_util.ArraysUtil;
import com.xhj_work_util.HuffmanTree;
import com.xhj_work_util.TreeNode;

public class job2_mapper extends Mapper<Object, Text, Text, Text> {
	HuffmanTree huffmanTree = new HuffmanTree();
	HashMap<String, TreeNode> word_map_treenode = null;

	@Override
	public void setup(Context context) {
		Path[] paths = null;
		try {
			paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		} catch (IOException e) {
			e.printStackTrace();
		}
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(paths[0].toString()));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			line = line.replaceFirst("\t", "   ");
			String[] one_line = ArraysUtil.process_arr(line.split(" "));

			if (one_line.length == 2) {
				huffmanTree.add_word_count(one_line[0], new Integer(one_line[1].trim()));
			}
			// System.out.println(line + "-----" + one_line.length + "-con-" +
			// ArraysUtil.arr_to_str(one_line));
		}

		huffmanTree.initilize_huffman_tree();
		word_map_treenode = huffmanTree.word_map_treenode();
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] vals = value.toString().split(" ");
		if (vals.length != 2) {
			String sor_word = vals[0];
			if (!word_map_treenode.containsKey(sor_word))
				return;
			TreeNode sor_node = word_map_treenode.get(sor_word);

			// context
			for (int i = 2; i < vals.length - 1; i++) {
				String context_word = vals[i];
				if (!word_map_treenode.containsKey(context_word))
					continue;

				TreeNode context_node = word_map_treenode.get(context_word);
				TreeNode root = huffmanTree.get_root();
				float[] e = new float[50];

				for (int j = 0; j < context_node.huf_code.length(); j++) {
					float q = (float) (1.0 / (1 + Math.pow(Math.E, ArraysUtil.multiply_arr(sor_node.vec, root.vec))));
					float g = (float) (0.6 * (1 - (context_node.huf_code.charAt(i) - '0') - q));
					e = ArraysUtil.add_arr(e, ArraysUtil.multiply_flo(root.vec, g));

					root.vec = ArraysUtil.add_arr(root.vec, ArraysUtil.multiply_flo(sor_node.vec, g));
					if (context_node.huf_code.charAt(i) - '0' == 0) {
						root = root.left;
					} else {
						root = root.right;
					}
				}
				sor_node.vec = ArraysUtil.add_arr(sor_node.vec, e);
			}
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<String, TreeNode> entry : word_map_treenode.entrySet()) {
			TreeNode node = entry.getValue();
			String val = "";
			for (int i = 0; i < node.vec.length; i++)
				val += " " + new Float(node.vec[i]).toString();
			context.write(new Text(entry.getKey()), new Text(val));
		}
	}

}