package cn.emay.store.file.queue;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import cn.emay.store.file.core.FileStoreItem;
import cn.emay.store.file.exception.FileStoreOutSizeException;
import cn.emay.store.file.util.ByteIntConverter;

/**
 * 历史数据读取工具,建议历史文件拷贝到其他位置进行读取，避免被删除逻辑影响
 * 
 * @author Frank
 *
 */
public class FileQueueHistoryReader {

	/**
	 * 读取，回调模式
	 * 
	 * @param dataFile
	 * @param handler
	 * @throws IOException
	 */
	public static void readHistory(File dataFile, HistoryDataHandler handler) {
		if (dataFile == null || !dataFile.exists()) {
			return;
		}
		FileStoreItem store = null;
		try {
			store = new FileStoreItem(dataFile, (int) dataFile.length(), FileChannel.MapMode.READ_ONLY);
			int nextBegin = 0;
			while (true) {
				try {
					if (nextBegin + 4 > store.getFileSize()) {
						break;
					}
					byte[] bytes = store.read(nextBegin, 4);
					int length = ByteIntConverter.toInt(bytes);
					if (length == 0) {
						break;
					}
					if (nextBegin + 4 + length > store.getFileSize()) {
						break;
					}
					bytes = store.read(nextBegin + 4, length);
					handler.handle(new String(bytes, "UTF-8"));
					nextBegin += 4 + length;
				} catch (FileStoreOutSizeException e) {
					break;
				}
			}
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		} finally {
			if (store != null) {
				store.close();
			}
		}
	}

}
