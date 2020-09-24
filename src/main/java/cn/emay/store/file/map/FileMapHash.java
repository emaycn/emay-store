package cn.emay.store.file.map;

import cn.emay.store.file.core.FileStoreItem;
import cn.emay.store.file.exception.FileStoreOutSizeException;
import cn.emay.store.file.util.ByteIntConverter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * FileMap 的 索引文件【定长文件】<br/>
 * item 列表<br/>
 * hasData(1),firstdatafileindex(4),firstdatabyteindex(4)
 *
 * @author Frank
 */
public class FileMapHash {

    /**
     * 文件
     */
    private final FileStoreItem store;

    /**
     * HASHING池大小
     */
    private final int hashLength;

    /**
     * 文件名
     */
    private final static String FILE_NAME = "emh";

    /**
     * 如果存在旧的hash，不会重新更改hash长度
     *
     * @param mapDirPath Map的路径
     * @param hashLength key分片数量
     * @throws IOException IO异常
     */
    protected FileMapHash(String mapDirPath, int hashLength) throws IOException {
        File file = new File(mapDirPath + File.separator + FILE_NAME);
        int oldLength = (int) (file.length() / 9);
        int fileSize;
        if (oldLength != 0 && oldLength != hashLength) {
            this.hashLength = oldLength;
            fileSize = oldLength * 9;
        } else {
            this.hashLength = hashLength;
            fileSize = hashLength * 9;
        }
        store = new FileStoreItem(file, fileSize);
    }

    /**
     * 获取key的链表首节点坐标
     *
     * @param key 键
     * @return [是否存在，文件序号，字节序号]
     */
    protected int[] readKeyCoordinate(byte[] key) {
        try {
            int hashing = hashing(key);
            byte[] bytes = store.read(hashing * 9, 9);
            int flag = bytes[0];
            if (flag == 0) {
                return new int[]{flag, 0, 0};
            }
            byte[] tmp = new byte[4];
            System.arraycopy(bytes, 1, tmp, 0, 4);
            int filePosition = ByteIntConverter.toInt(tmp);
            System.arraycopy(bytes, 5, tmp, 0, 4);
            int bytePosition = ByteIntConverter.toInt(tmp);
            return new int[]{flag, filePosition, bytePosition};
        } catch (FileStoreOutSizeException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 写入key的链表首节点坐标
     *
     * @param key          键
     * @param filePosition 文件序号
     * @param bytePosition 字节序号
     */
    protected synchronized void writeKeyCoordinate(byte[] key, int filePosition, int bytePosition) {
        int hashing = hashing(key);
        byte[] filePositionbytes = ByteIntConverter.toBytes(filePosition);
        byte[] bytePositionbytes = ByteIntConverter.toBytes(bytePosition);
        byte[] bytes = new byte[9];
        bytes[0] = 1;
        System.arraycopy(filePositionbytes, 0, bytes, 1, 4);
        System.arraycopy(bytePositionbytes, 0, bytes, 5, 4);
        try {
            store.write(hashing * 9, bytes);
        } catch (FileStoreOutSizeException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 删除key的链表首节点坐标
     *
     * @param key 键
     */
    protected synchronized void removeKeyCoordinate(byte[] key) {
        int hashing = hashing(key);
        try {
            store.write(hashing * 9, new byte[1]);
        } catch (FileStoreOutSizeException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 检测
     *
     * @param datas 所有数据文件
     * @return 文件数据量
     */
    protected Map<Integer, Integer> loadFileCounts(Map<Integer, FileMapData> datas) {
        Map<Integer, Integer> files = new HashMap<>(10);
        byte[] tmp = new byte[4];
        try {
            for (int i = 0; i < hashLength; i++) {
                byte[] bytes;
                bytes = store.read(i * 9, 9);
                int flag = bytes[0];
                if (flag == 0) {
                    continue;
                }
                System.arraycopy(bytes, 1, tmp, 0, 4);
                int filePosition = ByteIntConverter.toInt(tmp);
                System.arraycopy(bytes, 5, tmp, 0, 4);
                int bytePosition = ByteIntConverter.toInt(tmp);
                findNext(datas, files, filePosition, bytePosition);
            }
        } catch (FileStoreOutSizeException e) {
            throw new IllegalArgumentException(e);
        }
        return files;
    }

    /**
     * 查找下一个数据
     *
     * @param datas        数据文件
     * @param ccf          文件数据量
     * @param filePosition 文件游标
     * @param bytePosition 字节游标
     */
    private void findNext(Map<Integer, FileMapData> datas, Map<Integer, Integer> ccf, int filePosition, int bytePosition) {
        FileMapData data = datas.get(filePosition);
        if (data != null) {
            if (!ccf.containsKey(filePosition)) {
                ccf.put(filePosition, 0);
            }
            ccf.put(filePosition, ccf.get(filePosition) + 1);
            int[] ints = data.getNextIndex(bytePosition);
            if (ints[0] == 0) {
                return;
            }
            findNext(datas, ccf, ints[1], ints[2]);
        }
    }

    /**
     * 一致性哈希
     *
     * @param key 键
     * @return hash
     */
    private int hashing(byte[] key) {
        int hashCode = Math.abs(Arrays.hashCode(key));
        if (hashCode == Integer.MIN_VALUE) {
            hashCode = Integer.MAX_VALUE;
        }
        return hashCode % hashLength;
    }

    /**
     * 关闭Hash文件
     */
    protected synchronized void close() {
        store.close();
    }

    /**
     * 删除Hash文件
     */
    protected synchronized void delete() {
        store.delete();
    }

    /**
     * 同步Hash文件
     */
    protected synchronized void sync() {
        store.sync();
    }

}
