package cn.emay.store.file.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

import cn.emay.store.file.exception.FileStoreClosedException;
import cn.emay.store.file.exception.FileStoreOutSizeException;

/**
 * 存储文件
 * 
 * @author Frank
 *
 */
public class FileStoreItem {

	/**
	 * 文件
	 */
	private File file;
	/**
	 * NIO文件
	 */
	private RandomAccessFile raFile;
	/**
	 * NIO通道
	 */
	private FileChannel fc;
	/**
	 * 文件内存映射
	 */
	private MappedByteBuffer mappedByteBuffer;
	/**
	 * 每个文件大小
	 */
	private int fileSize;
	/**
	 * 是否关闭
	 */
	private boolean isClosed;

	/**
	 * 是否需要刷盘
	 */
	private boolean isNeedSync = false;
	
	/**
	 * 构造函数
	 * 
	 * @param file
	 *            文件
	 * @param fileSize
	 *            文件大小
	 * @throws IOException
	 */
	public FileStoreItem(File file, int fileSize) throws IOException {
		this(file, fileSize, FileChannel.MapMode.READ_WRITE);
	}

	/**
	 * 构造函数
	 * 
	 * @param file
	 *            文件
	 * @param fileSize
	 *            文件大小
	 * @throws IOException
	 */
	public FileStoreItem(File file, int fileSize,FileChannel.MapMode mode) throws IOException {
		this.file = file;
		if (!file.exists()) {
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			file.createNewFile();
		}
		if (fileSize < file.length()) {
			this.fileSize = (int) file.length();
		} else if (fileSize > Integer.MAX_VALUE) {
			this.fileSize = Integer.MAX_VALUE;
		} else {
			this.fileSize = fileSize;
		}
		raFile = new RandomAccessFile(file, "rwd");
		fc = raFile.getChannel();
		mappedByteBuffer = fc.map(mode, 0, this.fileSize);
		isClosed = false;
	}

	/**
	 * 检测关闭
	 */
	private void assertFileClosed() {
		if (isClosed) {
			throw new FileStoreClosedException();
		}
	}

	/**
	 * 关闭MappingBuffer
	 */
	private synchronized void closeBuffer() {
		if (mappedByteBuffer == null) {
			return;
		}
		AccessController.doPrivileged(new PrivilegedAction<Object>() {
			@SuppressWarnings("restriction")
			public Object run() {
				try {
					Method getCleanerMethod = mappedByteBuffer.getClass().getMethod("cleaner", new Class[0]);
					getCleanerMethod.setAccessible(true);
					sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(mappedByteBuffer, new Object[0]);
					cleaner.clean();
				} catch (Exception e) {
					throw new IllegalArgumentException(e);
				}
				return null;
			}
		});
	}

	/**
	 * 刷新文件大小<br/>
	 * 保留方法
	 * 
	 * @param fileSize
	 *            文件大小
	 * @throws IOException
	 */
	public synchronized void refulshFileSize(int fileSize) throws IOException {
		if (isClosed) {
			return;
		}
		sync(true);
		closeBuffer();
		this.fileSize = fileSize;
		mappedByteBuffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, this.fileSize);
	}

	/**
	 * 关闭文件
	 * 
	 * @throws IOException
	 */
	public synchronized void close() {
		if (isClosed) {
			return;
		}
		closeBuffer();
		if (fc != null) {
			try {
				fc.close();
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
			fc = null;
		}
		if (raFile != null) {
			try {
				raFile.close();
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
			raFile = null;
		}
		isClosed = true;
	}

	/**
	 * 删除文件
	 * 
	 * @throws IOException
	 */
	public synchronized void delete() {
		if (!isClosed) {
			close();
		}
		file.delete();
	}

	/**
	 * 更新是否需要同步
	 * 
	 * @param isNeedSync
	 */
	private synchronized void updateNeedSync(boolean isNeedSync) {
		this.isNeedSync = isNeedSync;
	}

	/**
	 * 同步文件
	 * 
	 * @param force
	 *            是否强制
	 */
	private synchronized void sync(boolean force) {
		if (isClosed) {
			return;
		}
		if (force || isNeedSync) {
			mappedByteBuffer.force();
			updateNeedSync(false);
		}
	}

	/**
	 * 同步文件【非强制】
	 */
	public synchronized void sync() {
		sync(false);
	}

	/**
	 * 写入数据
	 * 
	 * @param position
	 *            游标
	 * @param bytes
	 *            数据
	 * @throws FileStoreOutSizeException
	 */
	public synchronized void write(int position, byte[] bytes) throws FileStoreOutSizeException {
		assertFileClosed();
		if (bytes == null || bytes.length == 0) {
			return;
		}
		if (position < 0) {
			throw new IllegalArgumentException("position must not be less than 0");
		}
		if (bytes.length + position > fileSize) {
			throw new FileStoreOutSizeException();
		}
		mappedByteBuffer.position(position);
		mappedByteBuffer.put(bytes);
		updateNeedSync(true);
	}

	/**
	 * 读取数据
	 * 
	 * @param position
	 *            游标
	 * @param length
	 *            数据长度
	 * @return
	 * @throws FileStoreOutSizeException
	 */
	public synchronized byte[] read(int position, int length) throws FileStoreOutSizeException {
		assertFileClosed();
		if (position < 0) {
			throw new IllegalArgumentException("position must not be less than 0");
		}
		if (position + length > fileSize) {
			throw new FileStoreOutSizeException();
		}
		mappedByteBuffer.position(position);
		byte[] bytes = new byte[length];
		mappedByteBuffer.get(bytes);
		return bytes;
	}

	/**
	 * 文件路径
	 * 
	 * @return
	 */
	public String getFilePath() {
		return file.getAbsolutePath();
	}
	
	/**
	 * 文件
	 * 
	 * @return
	 */
	public File getFile() {
		return file;
	}

	/**
	 * 文件是否关闭
	 * 
	 * @return
	 */
	public boolean isClosed() {
		return isClosed;
	}

	/**
	 * 文件大小
	 * 
	 * @return
	 */
	public int getFileSize() {
		return fileSize;
	}

	/**
	 * 获取最后修改时间
	 * 
	 * @return
	 */
	public long getLastModifiedTime() {
		return file.lastModified();
	}

}
