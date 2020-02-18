package cn.emay.store.file.queue;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import cn.emay.store.file.exception.FileStoreClosedException;
import cn.emay.store.file.exception.FileStoreOutSizeException;
import cn.emay.store.file.util.Version1xCompatible;

/**
 * 文件队列<br/>
 * 
 * @author Frank
 * @throws IOException
 */
public class FileQueue implements Closeable{

	/**
	 * 默认数据文件长度【10m】
	 */
	protected final static int DEFAULT_FILE_SIZE = 1024 * 1024 * 10;

	/**
	 * 数据文件
	 */
	private Map<Integer, FileQueueData> datas = new ConcurrentHashMap<Integer, FileQueueData>();

	/**
	 * 统计文件
	 */
	private FileQueueInfo info;

	/**
	 * 是否关闭
	 */
	private boolean isClose;

	/**
	 * 文件大小
	 */
	private int oneDataFileSize = DEFAULT_FILE_SIZE;

	/**
	 * 路径
	 */
	private String queueDirPath;

	/**
	 * 刷盘时间间隔
	 */
	private long cleanUpPeriod = 30L * 1000L;

	/**
	 * 已经消费的数据，保留时间
	 */
	private long usedDataExpiryMill = -1L;

	/**
	 * 已经消费完的文件序号
	 */
	private TreeMap<Integer, File> oldList = new TreeMap<Integer, File>();

	/**
	 * 构造方法：开启启动检查、10M一个数据文件、30秒刷盘时间间隔、已消费数据不保留
	 * 
	 * @param queueDirPath
	 *            队列文件夹位置
	 */
	public FileQueue(String queueDirPath) {
		this(queueDirPath, 30, DEFAULT_FILE_SIZE, -1);
	}

	/**
	 * 
	 * @param queueDirPath
	 *            队列文件夹位置
	 * @param cleanUpPeriodSecond
	 *            刷盘时间间隔，秒【刷盘期间会影响数据读写速度，建议刷盘时间设置大一些】
	 * @param oneDataFileSize
	 *            单数据文件大小【堆积数据越多，单数据请设置越大，不要超过1G】
	 * @param usedDataExpirySecond
	 *            已经消费的数据，保留时间
	 */
	public FileQueue(String queueDirPath, int cleanUpPeriodSecond, int oneDataFileSize, int usedDataExpirySecond) {
		this.queueDirPath = queueDirPath;
		if (cleanUpPeriodSecond >= 1) {
			this.cleanUpPeriod = cleanUpPeriodSecond * 1000L;
		}
		if (oneDataFileSize > DEFAULT_FILE_SIZE) {
			this.oneDataFileSize = oneDataFileSize;
		}
		this.usedDataExpiryMill = usedDataExpirySecond * 1000L;
		try {
			File dir = loadDir(queueDirPath);
			info = new FileQueueInfo(queueDirPath);
			loadDataFiles(dir);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
		this.isClose = false;
		new Version1xCompatible().load1xQueue(queueDirPath, this);
		doSync();
	}

	/**
	 * 加载文件夹
	 * 
	 * @param queueDirPath
	 * @return
	 * @throws IOException
	 */
	private File loadDir(String queueDirPath) throws IOException {
		File folder = new File(queueDirPath);
		if (!folder.exists()) {
			folder.mkdirs();
		}
		if (!folder.isDirectory()) {
			throw new IOException("the same name [" + queueDirPath + "] file is exists .");
		}
		return folder;
	}

	/**
	 * 加载数据文件
	 * 
	 * @param dir
	 * @throws IOException
	 */
	private void loadDataFiles(File dir) throws IOException {
		File[] fileList = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(FileQueueData.END_FILE_NAME);
			}
		});
		if (fileList == null || fileList.length <= 0) {
			return;
		}
		Arrays.sort(fileList, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				try {
					int index1 = Integer.valueOf(o1.getName().replace(FileQueueData.END_FILE_NAME, ""));
					int index2 = Integer.valueOf(o2.getName().replace(FileQueueData.END_FILE_NAME, ""));
					return index1 > index2 ? 1 : -1;
				} catch (Exception e) {
					return -1;
				}
			}
		});
		for (File file : fileList) {
			int index = 0;
			try {
				index = Integer.valueOf(file.getName().replace(FileQueueData.END_FILE_NAME, ""));
			} catch (Exception e) {
				continue;
			}
			if (index < info.getNowReadFileIndex()) {
				oldList.put(index, file);
			} else {
				FileQueueData data = new FileQueueData(queueDirPath, oneDataFileSize, index);
				datas.put(index, data);
			}
		}
	}

	/**
	 * 同步方法
	 */
	private void doSync() {
		Thread t = new Thread("filequeue sync thread") {
			@Override
			public void run() {
				while (!isClose) {
					long stepSleep = 1000L;
					long hasSleep = 0L;
					while(hasSleep < cleanUpPeriod) {
						try {
							Thread.sleep(stepSleep);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						hasSleep += stepSleep;
						if(isClose) {
							break;
						}
					}
					if(isClose) {
						break;
					}
					try {
						sync();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		};
		t.setDaemon(true);
		t.start();
	}

	/**
	 * 同步逻辑
	 * 
	 */
	public void sync() {
		if (isClose) {
			return;
		}

		Map<Integer, File> olds = new HashMap<Integer, File>(10);
		for (Integer index : datas.keySet()) {
			if (index < info.getNowReadFileIndex()) {
				olds.put(index, datas.get(index).getFile());
			}
		}
		for (Integer index : olds.keySet()) {
			FileQueueData data = datas.get(index);
			if (data != null) {
				datas.remove(index);
				data.close();
			}
		}
		oldList.putAll(olds);

		List<Integer> deleteList = new ArrayList<Integer>();
		for (Integer index : oldList.keySet()) {
			if (System.currentTimeMillis() - oldList.get(index).lastModified() > usedDataExpiryMill) {
				deleteList.add(index);
			}
		}
		for (Integer index : deleteList) {
			File data = oldList.get(index);
			if (data != null && data.exists()) {
				boolean isDelete = false;
				try {
					isDelete = data.delete();
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (isDelete) {
					oldList.remove(index);
				}
			}
		}

		info.sync();
		for (FileQueueData fmd : datas.values()) {
			fmd.sync();
		}
	}

	/**
	 * 关闭
	 */
	@Override
	public synchronized void close() {
		if (isClose) {
			return;
		}
		info.close();
		for (FileQueueData fmd : datas.values()) {
			fmd.close();
		}
		isClose = true;
	}

	/**
	 * 删除
	 */
	public synchronized void delete() {
		this.close();
		info.delete();
		for (FileQueueData fmd : datas.values()) {
			fmd.delete();
		}
		for (File file : oldList.values()) {
			file.delete();
		}
		datas.clear();
		oldList.clear();
		info = null;
		new File(queueDirPath).delete();
	}

	/**
	 * 检测关闭
	 */
	private void assertFileClosed() {
		if (isClose) {
			throw new FileStoreClosedException();
		}
	}

	/**
	 * 创建新的数据文件
	 * 
	 * @return
	 * @throws IOException
	 */
	private synchronized FileQueueData createFileQueueData() throws IOException {
		info.addFile();
		FileQueueData data = new FileQueueData(queueDirPath, oneDataFileSize, info.getNowWriteFileIndex());
		datas.put(info.getNowWriteFileIndex(), data);
		return data;
	}

	/**
	 * 队列大小
	 * 
	 * @return
	 */
	public int size() {
		return info.getCount();
	}

	/**
	 * 压入数据
	 * 
	 * @param value
	 */
	public synchronized void offer(String value) {
		if (value == null) {
			return;
		}
		byte[] bytes;
		try {
			bytes = value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(e);
		}
		offerBytes(bytes);
	}

	/**
	 * 压入数据
	 * 
	 * @param value
	 */
	public synchronized void offerBytes(byte[] value) {
		assertFileClosed();
		if (value == null || value.length == 0) {
			return;
		}
		FileQueueData fmd = datas.get(info.getNowWriteFileIndex());
		try {
			if (fmd == null) {
				fmd = createFileQueueData();
			}
			fmd.write(info.getNowWriteByteIndex(), value);
			info.add(value.length);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		} catch (FileStoreOutSizeException e) {
			try {
				fmd = createFileQueueData();
			} catch (IOException e1) {
				throw new IllegalArgumentException(e1);
			}
			offerBytes(value);
		}
	}

	/**
	 * 弹出数据
	 * 
	 * @return
	 */
	public synchronized String poll() {
		byte[] bytes = pollBytes();
		if (bytes == null) {
			return null;
		}
		try {
			return new String(bytes, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 弹出数据
	 * 
	 * @return
	 */
	public synchronized byte[] pollBytes() {
		assertFileClosed();
		if (info.getNowReadFileIndex() >= info.getNowWriteFileIndex() && info.getNowReadByteIndex() >= info.getNowWriteByteIndex()) {
			info.updateCount(0);
			return null;
		}
		FileQueueData fmd = datas.get(info.getNowReadFileIndex());
		try {
			if (fmd == null) {
				return null;
			}
			byte[] bytes = fmd.read(info.getNowReadByteIndex());
			info.remove(bytes.length);
			return bytes;
		} catch (FileStoreOutSizeException e) {
			info.nextFile();
			return pollBytes();
		}
	}

	/**
	 * 获取所有历史数据文件
	 * 
	 * @return
	 */
	public List<File> getOldDataFiles() {
		assertFileClosed();
		List<File> files = new ArrayList<File>();
		files.addAll(oldList.values());
		return files;
	}

}
