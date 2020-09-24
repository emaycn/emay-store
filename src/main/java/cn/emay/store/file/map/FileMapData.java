package cn.emay.store.file.map;

import cn.emay.store.file.core.FileStoreItem;
import cn.emay.store.file.exception.FileStoreOutSizeException;
import cn.emay.store.file.util.ByteIntConverter;

import java.io.File;
import java.io.IOException;

/**
 * FileMap 数据文件【定长文件】<br/>
 * items :
 * length(4),hasNext(1),nextFileIndex(4),nextByteIndex(4),keylength(4),valuelength(4),key(keylength),value(valuelength)<br/>
 *
 * @author Frank
 */
public class FileMapData {

    /**
     * 文件名后缀
     */
    protected final static String END_FILE_NAME = ".emd";

    /**
     * 文件
     */
    private final FileStoreItem store;

    /**
     * @param mapDirPath Map的路径
     * @param fileSize   文件大小
     * @param index      data文件编号
     * @throws IOException IO异常
     */
    protected FileMapData(String mapDirPath, int fileSize, int index) throws IOException {
        File file = new File(mapDirPath + File.separator + index + END_FILE_NAME);
        store = new FileStoreItem(file, fileSize);
    }

    /**
     * 读取key
     *
     * @param readPosition 数据起始点
     * @return 数据对象
     */
    protected MapKeyData readKey(int readPosition) {
        if (readPosition < 0) {
            throw new IllegalArgumentException("position must not be less than 0");
        }
        if (readPosition + 21 > store.getFileSize()) {
            return null;
        }
        try {
            byte[] bytes = store.read(readPosition, 21);
            int posit = 0;
            byte[] readBytes = new byte[4];
            System.arraycopy(bytes, posit, readBytes, 0, 4);
            int length = ByteIntConverter.toInt(readBytes);
            if (length == 0) {
                return null;
            }
            posit += 4;
            boolean hasNext = bytes[posit] != 0;
            posit += 1;
            System.arraycopy(bytes, posit, readBytes, 0, 4);
            int nextFilePosition = ByteIntConverter.toInt(readBytes);
            posit += 4;
            System.arraycopy(bytes, posit, readBytes, 0, 4);
            int nextBytePosition = ByteIntConverter.toInt(readBytes);
            posit += 4;
            System.arraycopy(bytes, posit, readBytes, 0, 4);
            int keyLength = ByteIntConverter.toInt(readBytes);
            posit += 4;
            System.arraycopy(bytes, posit, readBytes, 0, 4);
            int valueLength = ByteIntConverter.toInt(readBytes);
            posit += 4;
            byte[] keyBytes = store.read(readPosition + posit, keyLength);
            int valuePosition = readPosition + posit + keyLength;
            return new MapKeyData(keyBytes, valuePosition, valueLength, hasNext, nextFilePosition, nextBytePosition);
        } catch (FileStoreOutSizeException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 读取Value
     *
     * @param valuePosition 值起始点
     * @param valueLength   值长度
     * @return 数据对象
     */
    protected byte[] readValue(int valuePosition, int valueLength) {
        if (valuePosition < 0) {
            throw new IllegalArgumentException("position must not be less than 0");
        }
        if (valuePosition + valueLength > store.getFileSize()) {
            return null;
        }
        try {
            return store.read(valuePosition, valueLength);
        } catch (FileStoreOutSizeException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 读取下一个节点的坐标
     *
     * @param readPosition 数据起始点
     * @return 数据坐标
     */
    protected int[] getNextIndex(int readPosition) {
        if (readPosition < 0) {
            throw new IllegalArgumentException("position must not be less than 0");
        }
        if (readPosition + 21 > store.getFileSize()) {
            return new int[]{0, 0, 0};
        }
        try {
            // 拿到总数据
            byte[] lengthBytes = store.read(readPosition, 4);
            int length = ByteIntConverter.toInt(lengthBytes);
            if (length == 0) {
                return new int[]{0, 0, 0};
            }
            byte[] bytes = store.read(readPosition + 4, length - 9);
            if (bytes[0] == 0) {
                return new int[]{0, 0, 0};
            }
            int posit = 1;
            byte[] readBytes = new byte[4];
            System.arraycopy(bytes, posit, readBytes, 0, 4);
            int nextFilePosition = ByteIntConverter.toInt(readBytes);
            posit += 4;
            System.arraycopy(bytes, posit, readBytes, 0, 4);
            int nextBytePosition = ByteIntConverter.toInt(readBytes);
            return new int[]{1, nextFilePosition, nextBytePosition};
        } catch (FileStoreOutSizeException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 写入数据
     *
     * @param writePosition 起始位置
     * @param key           键
     * @param value         值
     * @return 长度
     * @throws FileStoreOutSizeException 数据超出文件大小异常
     */
    protected synchronized int writeData(int writePosition, byte[] key, byte[] value) throws FileStoreOutSizeException {
        if (key == null || key.length == 0 || value == null || value.length == 0) {
            throw new IllegalArgumentException("key or value must not be null ");
        }
        if (writePosition < 0) {
            throw new IllegalArgumentException("writePosition must lagger than 0");
        }
        int totalLength = 21 + key.length + value.length;
        if (writePosition + totalLength > store.getFileSize()) {
            throw new FileStoreOutSizeException();
        }
        byte[] lengthBytes = ByteIntConverter.toBytes(totalLength);
        byte[] keyLengthBytes = ByteIntConverter.toBytes(key.length);
        byte[] valueLengthBytes = ByteIntConverter.toBytes(value.length);
        byte[] incom = new byte[totalLength];
        int posit = 0;
        System.arraycopy(lengthBytes, 0, incom, posit, 4);
        posit += 4;
        posit += 9;
        System.arraycopy(keyLengthBytes, 0, incom, posit, 4);
        posit += 4;
        System.arraycopy(valueLengthBytes, 0, incom, posit, 4);
        posit += 4;
        System.arraycopy(key, 0, incom, posit, key.length);
        posit += key.length;
        System.arraycopy(value, 0, incom, posit, value.length);
        store.write(writePosition, incom);
        return totalLength;
    }

    /**
     * 写入下一个节点信息
     *
     * @param position         节点游标
     * @param nextFilePosition 下一个节点文件编号
     * @param nextBytePosition 下一个节点游标
     */
    protected synchronized void writeNextPosition(int position, int nextFilePosition, int nextBytePosition) {
        if (position < 0) {
            throw new IllegalArgumentException("position must not be less than 0");
        }
        if (position + 21 > store.getFileSize()) {
            throw new IllegalArgumentException("position must  be less than filesize");
        }
        if (nextFilePosition < 0 || nextBytePosition < 0) {
            throw new IllegalArgumentException("nextFilePosition and nextBytePosition must not be less than 0");
        }
        try {
            byte[] tmp = new byte[9];
            byte[] filebytes = ByteIntConverter.toBytes(nextFilePosition);
            byte[] bytebytes = ByteIntConverter.toBytes(nextBytePosition);
            tmp[0] = 1;
            System.arraycopy(filebytes, 0, tmp, 1, 4);
            System.arraycopy(bytebytes, 0, tmp, 5, 4);
            store.write(position + 4, tmp);
        } catch (FileStoreOutSizeException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 删除节点的下一个节点信息
     *
     * @param position 节点游标
     */
    protected synchronized void removeNextPosition(int position) {
        if (position < 0) {
            throw new IllegalArgumentException("position must not be less than 0");
        }
        if (position + 21 > store.getFileSize()) {
            throw new IllegalArgumentException("position must  be less than filesize");
        }
        try {
            store.write(position + 4, new byte[1]);
        } catch (FileStoreOutSizeException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 关闭Data文件
     */
    protected synchronized void close() {
        store.close();
    }

    /**
     * 删除Data文件
     */
    protected synchronized void delete() {
        store.delete();
    }

    /**
     * 同步Data文件
     */
    protected synchronized void sync() {
        store.sync();
    }

}
