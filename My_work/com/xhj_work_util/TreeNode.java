package com.xhj_work_util;

public class TreeNode {
	public int count;
	public String huf_code;
	public String word;
	public float[] vec;
	public boolean is_leaf;

	// two branch
	public TreeNode left;
	public TreeNode right;

	static float[] gen_random_vec() {
		float[] re = new float[50];
		for (int i = 0; i < 50; i++) {
			re[i] = (float) Math.random();
		}
		return re;
	}

	// first construction
	public TreeNode(String word, int count) {
		is_leaf = true;
		this.word = word;
		this.count = count;
		this.vec = TreeNode.gen_random_vec();
		this.huf_code = "";

		this.left = null;
		this.right = null;
	}

	// second construction default
	public TreeNode() {
		this.count = 0;
		this.huf_code = "";
		this.word = "";
		this.vec = TreeNode.gen_random_vec();
		this.is_leaf = false;

		this.left = null;
		this.right = null;
	}

	// merge
	public static TreeNode merge(TreeNode th, TreeNode other) {
		TreeNode temp = new TreeNode();

		temp.count = th.count + other.count;
		temp.left = th;
		temp.right = other;

		temp.left.huf_code = "0" + temp.left.huf_code;
		temp.right.huf_code = "1" + temp.right.huf_code;

		return temp;
	}
}
