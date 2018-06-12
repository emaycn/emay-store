package cn.emay.store.file.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import cn.emay.store.file.core.FileStoreItem;
import cn.emay.store.file.exception.FileStoreOutSizeException;
import cn.emay.store.file.map.FileMap;
import cn.emay.store.file.queue.FileQueue;

/**
 * 1.x版本兼容
 * 
 * @author Frank
 *
 */
public class Version1xCompatible {

	/**
	 * 加载1.x版本的map
	 * 
	 * @param dirPath
	 * @param map
	 */
	public void load1xMap(String dirPath, FileMap map) {
		File dir = new File(dirPath);
		if (!dir.exists()) {
			return;
		}
		if (!dir.isDirectory()) {
			return;
		}
		File[] datas = dir.listFiles();
		if (datas == null || datas.length <= 0) {
			return;
		}
		List<File> list = new ArrayList<File>();
		for (File fileone : datas) {
			if (fileone.getName().startsWith("map.")) {
				list.add(fileone);
			}
		}
		if (list.size() <= 0) {
			return;
		}
		Collections.sort(list, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				int i = Integer.valueOf(o1.getName().replace("map.", ""));
				int j = Integer.valueOf(o2.getName().replace("map.", ""));
				return i > j ? 1 : -1;
			}
		});
		try {
			for (File file : list) {
				long fileLength = file.length();
				FileStoreItem store = new FileStoreItem(file, (int) fileLength);
				int position = 0;
				while (true) {
					if (position + 4 > fileLength) {
						break;
					}
					byte[] keylengthbytes = store.read(position, 4);
					if (isZero(keylengthbytes)) {
						break;
					}
					int keylength = ByteIntConverter.toInt(keylengthbytes);
					if (keylength <= 0) {
						break;
					}
					position += 4;
					byte[] keybytes = store.read(position, keylength);
					position += keylength;
					byte[] valuelengthbytes = store.read(position, 4);
					int valuelength = ByteIntConverter.toInt(valuelengthbytes);
					position += 4;
					byte[] valuebytes = store.read(position, valuelength);
					position += valuelength;
					String key = new String(keybytes, "UTF-8");
					String value = new String(valuebytes, "UTF-8");
					map.put(key, value);
				}
				store.delete();
			}
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 加载1.x版本的queue
	 * 
	 * @param dirPath
	 * @param queue
	 */
	public void load1xQueue(String dirPath, FileQueue queue) {
		File dir = new File(dirPath);
		if (!dir.exists()) {
			return;
		}
		if (!dir.isDirectory()) {
			return;
		}
		File[] datas = dir.listFiles();
		if (datas == null || datas.length <= 0) {
			return;
		}
		List<File> list = new ArrayList<File>();
		for (File fileone : datas) {
			if (fileone.getName().startsWith("queue.")) {
				list.add(fileone);
			}
		}
		if (list.size() <= 0) {
			return;
		}
		Collections.sort(list, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				int i = Integer.valueOf(o1.getName().replace("queue.", ""));
				int j = Integer.valueOf(o2.getName().replace("queue.", ""));
				return i > j ? 1 : -1;
			}
		});
		try {
			for (File file : list) {
				long fileLength = file.length();
				FileStoreItem store = new FileStoreItem(file, (int) fileLength);
				int position = 0;
				while (true) {
					if (position + 4 > fileLength) {
						break;
					}
					byte[] lengthbytes = store.read(position, 4);
					if (isZero(lengthbytes)) {
						break;
					}
					int length = ByteIntConverter.toInt(lengthbytes);
					if (position + 4 + length > fileLength) {
						break;
					}
					position += 4;
					byte[] valuebytes = store.read(position, length);
					position += length;
					String value = new String(valuebytes, "UTF-8");
					queue.offer(value);
				}
				store.delete();
			}
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		} catch (FileStoreOutSizeException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 是否全是0
	 * 
	 * @param bytes
	 * @return
	 */
	public boolean isZero(byte[] bytes) {
		boolean isNull = true;
		if (bytes == null || bytes.length == 0) {
			return isNull;
		}
		for (byte b : bytes) {
			if (b != 0) {
				isNull = false;
				break;
			}
		}
		return isNull;
	}

}
