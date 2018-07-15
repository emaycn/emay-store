package cn.emay.store.file.map;

/**
 * 数据节点【除了value以外的其他信息】
 * 
 * @author Frank
 *
 */
public class MapKeyData {

	/**
	 * 键
	 */
	private byte[] key;

	/**
	 * 值起始坐标
	 */
	private int valuePosition;

	/**
	 * 值长度
	 */
	private int valueLength;

	/**
	 * 是否有下一个节点
	 */
	private boolean hasNext;

	/**
	 * 下一个节点文件编号
	 */
	private int nextFilePosition;

	/**
	 * 下一个节点游标
	 */
	private int nextBytePosition;

	/**
	 * 
	 * @param key
	 *            键
	 * @param valuePosition
	 *            值起始坐标
	 * @param valueLength
	 *            值长度
	 * @param hasNext
	 *            是否有下一个节点
	 * @param nextFilePosition
	 *            下一个节点文件编号
	 * @param nextBytePosition
	 *            下一个节点游标
	 */
	public MapKeyData(byte[] key, int valuePosition, int valueLength, boolean hasNext, int nextFilePosition, int nextBytePosition) {
		this.key = key;
		this.valuePosition = valuePosition;
		this.valueLength = valueLength;
		this.nextFilePosition = nextFilePosition;
		this.nextBytePosition = nextBytePosition;
		this.hasNext = hasNext;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public int getNextFilePosition() {
		return nextFilePosition;
	}

	public void setNextFilePosition(int nextFilePosition) {
		this.nextFilePosition = nextFilePosition;
	}

	public int getNextBytePosition() {
		return nextBytePosition;
	}

	public void setNextBytePosition(int nextBytePosition) {
		this.nextBytePosition = nextBytePosition;
	}

	public boolean isHasNext() {
		return hasNext;
	}

	public void setHasNext(boolean hasNext) {
		this.hasNext = hasNext;
	}

	public int getValuePosition() {
		return valuePosition;
	}

	public void setValuePosition(int valuePosition) {
		this.valuePosition = valuePosition;
	}

	public int getValueLength() {
		return valueLength;
	}

	public void setValueLength(int valueLength) {
		this.valueLength = valueLength;
	}

}
