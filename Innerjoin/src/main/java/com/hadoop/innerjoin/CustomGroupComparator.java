package com.hadoop.innerjoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



public class CustomGroupComparator extends WritableComparator {

	protected CustomGroupComparator() {
		super(TextPair.class, true);
	}
	
	/*As i initially assumed that i could use a delimited Text as the key as an output of a mapper
	 * and then split it at the GroupComparator and KeyComparator levels will not work as the 
	 * compare method is invoked only on WritableComparable 
	 * So public int compare(Text keyA, keyB) will not be invoked.
	 * 
	 * Also,
	 * The custom type (for a Pair of values together as a key) that you will need to create to represent the compositeKey needs to
	 * 1. implement WritableComaprable
	 * 2. override compareTo methods for comparing this compositeKey based on
	 * 		a. Both first and second values
	 * 		b. Only on the first value
	 * 		c. Only on the second value 
	 * 
	 * In this Project we had a use a key as a composite key. (deptId + [0|1])
	 * For achieving this key we created a class com.cloudwick.mapreduce.customType.TextPair
	 * 1. It implements WritableComparable
	 * 2. It overrides compareTo for comparing both first and second values.
	 * 		public int compareTo(TextPair tp)
	 * 3. It overrides compareTo for comparing either only first or only second values.
	 * 		public int compareTo(TextPair tp, int index)
	 * */
	
	public int compare(WritableComparable compositeKey1, WritableComparable compositeKey2)
	{
		TextPair keyA = (TextPair)compositeKey1;
		TextPair keyB = (TextPair)compositeKey2;
		return keyA.compareTo(keyB, 1);
		
	}

}
