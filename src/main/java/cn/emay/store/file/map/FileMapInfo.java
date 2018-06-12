
package cn.emay.store.file.map;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import cn.emay.store.file.core.FileStoreItem;
import cn.emay.store.file.exception.FileStoreOutSizeException;
import cn.emay.store.file.util.ByteIntConverter;

/**
 * FileMap 的 计数文件【定长文件】<br/>
 * <br/>
 * nowFileIndex(4),nowByteIndex(4),count(4)<br/>
 * 
 * @author Frank
 *
 */
public class FileMapInfo {

	/**
	 * 当前文件编号
	 */
	private int nowFileIndex = 0;

	/**
	 * 当前数据编号
	 */
	private int nowByteIndex = 0;

	/**
	 * 总数据量
	 */
	private int count = 0;

	/**
	 * 文件数据量
	 */
	private Map<Integer, Integer> fileCounts = new ConcurrentHashMap<Integer, Integer>();

	/**
	 * 存储文件
	 */
	private FileStoreItem store;

	/**
	 * 文件名
	 */
	private final static String FILE_NAME = "emc";

	/**
	 * 默认文件长度
	 */
	private int fileSize = 12;

	/**
	 * 
	 * @param mapDirPath
	 *            Map的路径
	 * @throws IOException
	 */
	protected FileMapInfo(String mapDirPath) throws IOException {
		File file = new File(mapDirPath + File.separator + FILE_NAME);
		store = new FileStoreItem(file, fileSize);
		try {
			int begin = 0;
			byte[] bytes = store.read(0, 12);
			byte[] tmp = new byte[4];
			System.arraycopy(bytes, begin, tmp, 0, 4);
			this.nowFileIndex = ByteIntConverter.toInt(tmp);
			begin += 4;
			System.arraycopy(bytes, begin, tmp, 0, 4);
			this.nowByteIndex = ByteIntConverter.toInt(tmp);
			begin += 4;
			System.arraycopy(bytes, begin, tmp, 0, 4);
			this.count = ByteIntConverter.toInt(tmp);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 增加数据文件
	 */
	protected void addFile() {
		this.nowFileIndex++;
		this.nowByteIndex = 0;
		byte[] nowFileIndexBytes = ByteIntConverter.toBytes(nowFileIndex);
		byte[] nowByteIndexBytes = ByteIntConverter.toBytes(nowByteIndex);
		byte[] incom = new byte[8];
		System.arraycopy(nowFileIndexBytes, 0, incom, 0, 4);
		System.arraycopy(nowByteIndexBytes, 0, incom, 4, 4);
		try {
			store.write(0, incom);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
		fileCounts.put(this.nowFileIndex, 0);
	}

	/**
	 * 数据量加一
	 */
	protected synchronized void add(int length) {
		count++;
		this.nowByteIndex += length;
		byte[] nowFileIndexBytes = ByteIntConverter.toBytes(nowFileIndex);
		byte[] nowByteIndexBytes = ByteIntConverter.toBytes(nowByteIndex);
		byte[] countBytes = ByteIntConverter.toBytes(count);
		byte[] incom = new byte[fileSize];
		System.arraycopy(nowFileIndexBytes, 0, incom, 0, 4);
		System.arraycopy(nowByteIndexBytes, 0, incom, 4, 4);
		System.arraycopy(countBytes, 0, incom, 8, 4);
		try {
			store.write(0, incom);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
		if(!fileCounts.containsKey(this.nowFileIndex)) {
			fileCounts.put(this.nowFileIndex, 0);
		}else {
			fileCounts.put(this.nowFileIndex, fileCounts.get(this.nowFileIndex) + 1);
		}
	}

	/**
	 * 数据量减一
	 * 
	 * @param fileIndex
	 *            文件序号
	 */
	protected synchronized void remove(int fileIndex) {
		count--;
		byte[] countBytes = ByteIntConverter.toBytes(count);
		try {
			store.write(8, countBytes);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
		if(!fileCounts.containsKey(fileIndex)) {
			fileCounts.put(fileIndex, 0);
		}else {
			fileCounts.put(fileIndex, fileCounts.get(fileIndex) - 1);
		}
	}

	/**
	 * 获取map总大小
	 * 
	 * @return
	 */
	protected int getCount() {
		return count;
	}

	/**
	 * 获取文件数据量
	 * 
	 * @return
	 */
	protected synchronized Map<Integer, Integer> getFileCounts() {
		return fileCounts;
	}
	
	/**
	 * 获取文件数据量
	 * 
	 * @return
	 */
	protected synchronized void removeFileCount(int fileIndex) {
		 fileCounts.remove(fileIndex);
	}

	/**
	 * 更新文件数据量
	 * 
	 * @param fileCounts
	 */
	protected synchronized void setFileCounts(Map<Integer, Integer> fileCounts) {
		this.fileCounts = fileCounts;
		int total = 0 ;
		for(Entry<Integer, Integer> s : fileCounts.entrySet()) {
			total += s.getValue();
		}
		this.count = total;
	}

	/**
	 * 获取当前文件编号
	 * 
	 * @return
	 */
	protected synchronized int getNowFileIndex() {
		return nowFileIndex;
	}

	/**
	 * 获取当前数据编号
	 * 
	 * @return
	 */
	protected synchronized int getNowByteIndex() {
		return nowByteIndex;
	}

	/**
	 * 关闭
	 */
	protected synchronized void close() {
		store.close();
	}

	/**
	 * 删除
	 */
	protected synchronized void delete() {
		store.delete();
	}

	/**
	 * 同步
	 */
	protected synchronized void sync() {
		store.sync();
	}

}
