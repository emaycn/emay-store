package cn.emay.store.file.queue;

import java.io.File;
import java.io.IOException;

import cn.emay.store.file.core.FileStoreItem;
import cn.emay.store.file.exception.FileStoreOutSizeException;
import cn.emay.store.file.util.ByteIntConverter;

/**
 * FileQueue 的 计数文件【定长文件】<br/>
 * <br/>
 * nowReadFileIndex(4),nowReadByteIndex(4),count(4),nowWriteByteIndex(4),nowWriteFileIndex(4)<br/>
 * 
 * @author Frank
 *
 */
public class FileQueueInfo {

	/**
	 * 当前读取的文件编号
	 */
	private int nowReadFileIndex = 1;

	/**
	 * 当前读取的数据游标
	 */
	private int nowReadByteIndex = 0;

	/**
	 * 当前写入的文件编号
	 */
	private int nowWriteFileIndex = 0;

	/**
	 * 当前写入的数据游标
	 */
	private int nowWriteByteIndex = 0;

	/**
	 * 文件大小
	 */
	private int fileSize = 20;

	/**
	 * 总数
	 */
	private int count = 0;

	/**
	 * 存储文件
	 */
	private FileStoreItem store;

	/**
	 * 文件名
	 */
	private final static String FILE_NAME = "eqc";

	/**
	 * 
	 * @param queueDirPath
	 *            Queue的路径
	 * @throws IOException
	 */
	protected FileQueueInfo(String queueDirPath) throws IOException {
		File file = new File(queueDirPath + File.separator + FILE_NAME);
		this.store = new FileStoreItem(file, fileSize);
		try {
			int begin = 0;
			byte[] bytes = store.read(0, fileSize);
			byte[] tmp = new byte[4];
			System.arraycopy(bytes, begin, tmp, 0, 4);
			this.nowReadFileIndex = ByteIntConverter.toInt(tmp);
			this.nowReadFileIndex = this.nowReadFileIndex == 0 ? 1 : this.nowReadFileIndex;
			begin += 4;
			System.arraycopy(bytes, begin, tmp, 0, 4);
			this.nowReadByteIndex = ByteIntConverter.toInt(tmp);
			begin += 4;
			System.arraycopy(bytes, begin, tmp, 0, 4);
			this.count = ByteIntConverter.toInt(tmp);
			begin += 4;
			System.arraycopy(bytes, begin, tmp, 0, 4);
			this.nowWriteByteIndex = ByteIntConverter.toInt(tmp);
			begin += 4;
			System.arraycopy(bytes, begin, tmp, 0, 4);
			this.nowWriteFileIndex = ByteIntConverter.toInt(tmp);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 新增一个文件
	 * 
	 * @throws IOException
	 */
	protected synchronized void addFile() {
		this.nowWriteByteIndex = 0;
		this.nowWriteFileIndex++;
		byte[] nowWriteByteIndextmp = ByteIntConverter.toBytes(this.nowWriteByteIndex);
		byte[] nowWriteFileIndextmp = ByteIntConverter.toBytes(this.nowWriteFileIndex);
		byte[] bytes = new byte[8];
		System.arraycopy(nowWriteByteIndextmp, 0, bytes, 0, 4);
		System.arraycopy(nowWriteFileIndextmp, 0, bytes, 4, 4);
		try {
			this.store.write(12, bytes);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 切换到下一个文件
	 * 
	 * @throws IOException
	 */
	protected synchronized void nextFile() {
		this.nowReadFileIndex++;
		this.nowReadByteIndex = 0;
		byte[] nowReadFileIndextmp = ByteIntConverter.toBytes(this.nowReadFileIndex);
		byte[] nowReadByteIndextmp = ByteIntConverter.toBytes(this.nowReadByteIndex);
		byte[] bytes = new byte[8];
		System.arraycopy(nowReadFileIndextmp, 0, bytes, 0, 4);
		System.arraycopy(nowReadByteIndextmp, 0, bytes, 4, 4);
		try {
			this.store.write(0, bytes);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 新增数据
	 * 
	 * @param bytesLength
	 *            数据长度
	 */
	protected synchronized void add(int bytesLength) {
		count++;
		nowWriteByteIndex += bytesLength + 4;
		byte[] counttmp = ByteIntConverter.toBytes(this.count);
		byte[] nowWriteByteIndextmp = ByteIntConverter.toBytes(this.nowWriteByteIndex);
		byte[] bytes = new byte[8];
		System.arraycopy(counttmp, 0, bytes, 0, 4);
		System.arraycopy(nowWriteByteIndextmp, 0, bytes, 4, 4);
		try {
			this.store.write(8, bytes);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 删除数据
	 * 
	 * @param bytesLength
	 *            数据长度
	 */
	protected synchronized void remove(int bytesLength) {
		this.nowReadByteIndex += bytesLength + 4;
		count--;
		byte[] nowReadByteIndextmp = ByteIntConverter.toBytes(this.nowReadByteIndex);
		byte[] counttmp = ByteIntConverter.toBytes(this.count);
		byte[] bytes = new byte[8];
		System.arraycopy(nowReadByteIndextmp, 0, bytes, 0, 4);
		System.arraycopy(counttmp, 0, bytes, 4, 4);
		try {
			this.store.write(4, bytes);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 更新总数
	 * 
	 * @param bytesLength
	 *            数据长度
	 */
	protected synchronized void updateCount(int count) {
		if (count == this.count) {
			return;
		}
		this.count = count;
		byte[] countbytes = ByteIntConverter.toBytes(this.count);
		try {
			store.write(16, countbytes);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 当前读取的文件编号
	 * 
	 * @return
	 */
	protected int getNowReadFileIndex() {
		return nowReadFileIndex;
	}

	/**
	 * 当前读取的数据游标
	 * 
	 * @return
	 */
	protected int getNowReadByteIndex() {
		return nowReadByteIndex;
	}

	/**
	 * 当前写入的文件编号
	 * 
	 * @return
	 */
	protected int getNowWriteFileIndex() {
		return nowWriteFileIndex;
	}

	/**
	 * 当前写入的数据游标
	 * 
	 * @return
	 */
	protected int getNowWriteByteIndex() {
		return nowWriteByteIndex;
	}

	/**
	 * 当前写入的数据游标
	 * 
	 * @return
	 */
	protected int getCount() {
		return count;
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
