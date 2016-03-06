package com.xhj_work_util;

public class ArraysUtil {

	public static float multiply_arr(float[] one, float[] two) {
		float re = 0.0f;
		for (int i = 0; i < one.length; i++) {
			re += one[i] * two[i];
		}
		return re;
	}

	public static float[] multiply_flo(float[] one, float f) {
		for (int i = 0; i < one.length; i++) {
			one[i] = one[i] * f;
		}
		return one;
	}

	public static float[] add_arr(float[] one, float[] two) {
		float[] re = new float[one.length];
		for (int i = 0; i < one.length; i++) {
			re[i] = one[i] + two[i];
		}
		return re;
	}

	public static String arr_to_str(String[] arr) {
		String re = "";
		for (int i = 0; i < arr.length; i++) {
			re += "[" + arr[i] + "]";
		}
		return re;
	}

	public static String[] process_arr(String[] arr) {
		String re = "";
		for (int i = 0; i < arr.length; i++) {
			if (arr[i].length() != 0)
				re += " " + arr[i];
		}

		if (re.length() != 0) {
			return re.substring(1).split(" ");
		}
		return re.split(" ");
	}
}
