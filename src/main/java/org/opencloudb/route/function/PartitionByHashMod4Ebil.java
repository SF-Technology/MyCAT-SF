package org.opencloudb.route.function;

import org.opencloudb.config.model.rule.RuleAlgorithm;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 哈希值取模
 * 根据分片列的哈希值对分片个数取模，哈希算法为Wang/Jenkins
 * 用法和简单取模相似，规定分片个数和分片列即可。
 *
 * @author Hash Zhang
 */
public class PartitionByHashMod4Ebil extends AbstractPartitionAlgorithm implements RuleAlgorithm {

	private static final String DELIMITER = ",";

	private boolean watch = false;
	private String excludedColumnValue;
	private Set<String> excludedColumnSet;
	private int count;

	public void setCount(int count) {
		this.count = count;
		if ((count & (count - 1)) == 0) {
			watch = true;
		}
	}

	public void setExcludedColumnValue(String excludedColumnValue) {
		this.excludedColumnValue = excludedColumnValue;
		this.excludedColumnSet = new HashSet<String>(
				Arrays.asList(excludedColumnValue.trim().split("\\s*" + DELIMITER + "\\s*")));
	}

	public String getExcludedColumnValue() {
		return excludedColumnValue;
	}

	public int getCount() {
		return count;
	}

	/**
	 * Using Wang/Jenkins Hash
	 *
	 * @param key
	 * @return hash value
	 */
	protected int hash(int key) {
		key = (~key) + (key << 21); // key = (key << 21) - key - 1;
		key = key ^ (key >> 24);
		key = (key + (key << 3)) + (key << 8); // key * 265
		key = key ^ (key >> 14);
		key = (key + (key << 2)) + (key << 4); // key * 21
		key = key ^ (key >> 28);
		key = key + (key << 31);
		return key;
	}

	/**
	 * 
	 * 被排除的列值直接进入最后一个分区，其余的哈希值取模
	 *
	 * @author Hash Zhang
	 */
	@Override
	public Integer calculate(String columnValue) {
		// columnValue = columnValue.replace("\'", " ");
		// columnValue = columnValue.trim();
		int newCount = count - 1;
		if (excludedColumnSet.contains(columnValue)) {
			return newCount;
		}
		BigInteger bigNum = new BigInteger(hash(columnValue.hashCode()) + "").abs();
		// if count==2^n, then m%count == m&(count-1)
		if (watch) {
			return (bigNum.intValue() & (newCount - 1));
		}
		return (bigNum.mod(BigInteger.valueOf(newCount))).intValue();
	}

	@Override
	public void init() {
		super.init();
	}

	@Override
	public int requiredNodeNum() {
		return this.count;
	}

}