package cn.emay.store.file.queue;

import cn.emay.store.file.core.FileStoreItem;
import cn.emay.store.file.exception.FileStoreOutSizeException;
import cn.emay.store.file.util.ByteIntConverter;

import java.io.File;
import java.io.IOException;

/**
 * FileQueue 的数据文件【定长文件】<br/>
 * <br/>
 * item:<br/>
 * length(4),value(length)<br/>
 *
 * @author Frank
 */
public class FileQueueData {

    /**
     * 文件
     */
    private final FileStoreItem store;
    /**
     * 文件名后缀
     */
    protected final static String END_FILE_NAME = ".eqd";

    /**
     * @param dataFile 数据文件
     * @throws IOException IO异常
     */
    protected FileQueueData(File dataFile) throws IOException {
        store = new FileStoreItem(dataFile, (int) dataFile.length());
    }

    /**
     * @param queueDirPath 队列文件夹
     * @param fileSize     单数据文件大小
     * @param index        当前文件编号
     * @throws IOException IO异常
     */
    protected FileQueueData(String queueDirPath, int fileSize, int index) throws IOException {
        File file = new File(queueDirPath + File.separator + index + END_FILE_NAME);
        store = new FileStoreItem(file, fileSize);
    }

    /**
     * 写入数据
     *
     * @param writePosition 写入开始游标
     * @param bytes         数据
     * @throws FileStoreOutSizeException 数据超出文件大小异常
     */
    protected synchronized void write(int writePosition, byte[] bytes) throws FileStoreOutSizeException {
        if (bytes == null || bytes.length == 0) {
            return;
        }
        if (writePosition < 0) {
            throw new IllegalArgumentException("writePosition must bigger than 0");
        }
        int nextwritePosition = bytes.length + 4 + writePosition;
        if (nextwritePosition > store.getFileSize()) {
            throw new FileStoreOutSizeException();
        }
        byte[] length = ByteIntConverter.toBytes(bytes.length);
        byte[] incom = new byte[bytes.length + 4];
        System.arraycopy(length, 0, incom, 0, 4);
        System.arraycopy(bytes, 0, incom, 4, bytes.length);
        store.write(writePosition, incom);
    }

    /**
     * 读取数据
     *
     * @param readPosition 读取开始游标
     * @return 数据
     * @throws FileStoreOutSizeException 数据超出文件大小异常
     */
    protected byte[] read(int readPosition) throws FileStoreOutSizeException {
        int nextwritePosition = readPosition + 4;
        if (nextwritePosition > store.getFileSize()) {
            throw new FileStoreOutSizeException();
        }
        byte[] lengthbytes = store.read(readPosition, 4);
        int length = ByteIntConverter.toInt(lengthbytes);
        if (length == 0) {
            throw new FileStoreOutSizeException();
        }
        return store.read(readPosition + 4, length);
    }

    /**
     * 读取文件包含的数据数量<br/>
     * 保留方法：遍历所有数据，校正总数，暂时不需要；
     *
     * @param beginPosition 起始游标
     * @return 数据数量
     */
    protected int readDataInfo(int beginPosition) {
        int count = 0;
        int readPosition = beginPosition;
        long size = store.getFileSize();
        while (readPosition + 4 < size) {
            try {
                byte[] lengthbytes = store.read(readPosition, 4);
                int length = ByteIntConverter.toInt(lengthbytes);
                if (length <= 0) {
                    break;
                }
                readPosition += 4 + length;
                count++;
            } catch (FileStoreOutSizeException e) {
                throw new IllegalArgumentException(e);
            }
        }
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

    /**
     * 获取文件
     *
     * @return 文件
     */
    protected File getFile() {
        return store.getFile();
    }

}
