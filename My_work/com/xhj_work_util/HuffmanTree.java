package com.xhj_work_util;

import java.util.ArrayList;
import java.util.HashMap;

public class HuffmanTree {
	ArrayList<TreeNode> con = new ArrayList<TreeNode>();

	// does not useful
	public HuffmanTree() {
	}

	public void add_word_count(String word, int count) {
		con.add(new TreeNode(word, count));
	}

	// init huffman tree, let con left only one node
	public void initilize_huffman_tree() {
		while (con.size() > 1) {
			TreeNode one = sel_min();
			TreeNode two = sel_min();
			con.add(TreeNode.merge(one, two));
		}
	}

	// select minist count tree node
	public TreeNode sel_min() {
		TreeNode min = null;
		for (TreeNode node : con) {
			if (min == null) {
				min = node;
			} else if (min.count < node.count) {
				min = node;
			}
		}
		con.remove(min);
		return min;
	}

	// transfer
	public HashMap<String, TreeNode> word_map_treenode() {
		HashMap<String, TreeNode> re = new HashMap<String, TreeNode>();
		tran_leaf(con.get(0), re);
		return re;
	}

	void tran_leaf(TreeNode root, HashMap<String, TreeNode> re) {
		if (root.left == null && root.right == null) {
			re.put(root.word, root);
			return;
		}
		if (root.left != null)
			tran_leaf(root.left, re);
		if (root.right != null)
			tran_leaf(root.right, re);
	}

	// get root
	public TreeNode get_root() {
		return con.get(0);
	}
}
