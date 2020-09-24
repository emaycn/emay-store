package cn.emay.store.file.map;

import cn.emay.store.file.exception.FileStoreClosedException;
import cn.emay.store.file.exception.FileStoreOutSizeException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 基于文件存储的Map<br/>
 * 由hash文件(1)、计数文件(1)、数据文件(n)三类文件构成<br/>
 *
 * @author Frank
 */
public class FileMap implements Closeable {

    /**
     * 默认数据文件长度【10m】
     */
    protected final static int DEFAULT_FILE_SIZE = 1024 * 1024 * 10;
    /**
     * 默认HASH文件长度【1m】
     */
    protected final static int DEFAULT_HASH_SIZE = 1024 * 1024;

    /**
     * hash文件
     */
    private FileMapHash hash;
    /**
     * 数据文件
     */
    private final Map<Integer, FileMapData> datas = new ConcurrentHashMap<>();
    /**
     * 统计文件
     */
    private FileMapInfo info;
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
    private final String mapDirPath;
    /**
     * 清理线程
     */
    private ScheduledExecutorService executorService;


    /**
     * @param mapDirPath Map文件夹地址
     */
    public FileMap(String mapDirPath, int cleanUpPeriodSecond, int oneDataFileSize) {
        this(mapDirPath, cleanUpPeriodSecond, oneDataFileSize, DEFAULT_HASH_SIZE);
    }

    /**
     * 默认构造【60秒同步一次磁盘、10M一个数据文件、1024*1024个key分片】
     *
     * @param mapDirPath Map文件夹地址
     */
    public FileMap(String mapDirPath) {
        this(mapDirPath, 60, DEFAULT_FILE_SIZE, DEFAULT_HASH_SIZE);
    }

    /**
     * @param mapDirPath          Map文件夹地址
     * @param cleanUpPeriodSecond 刷盘时间间隔，秒，小于等于0则不主动落盘
     * @param oneDataFileSize     单个数据文件大小
     * @param hashLength          key分片数量，重启后如果跟之前的分片数量不一致，采用之前的分片数量
     */
    public FileMap(String mapDirPath, int cleanUpPeriodSecond, int oneDataFileSize, int hashLength) {
        this.mapDirPath = mapDirPath;
        if (oneDataFileSize > DEFAULT_FILE_SIZE) {
            this.oneDataFileSize = oneDataFileSize;
        }
        try {
            File folder = loadDir(mapDirPath);
            info = new FileMapInfo(mapDirPath);
            hash = new FileMapHash(mapDirPath, hashLength);
            loadDataFiles(folder);
            Map<Integer, Integer> map = hash.loadFileCounts(datas);
            info.setFileCounts(map);
            this.isClose = false;
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        if (cleanUpPeriodSecond > 0) {
            executorService = Executors.newScheduledThreadPool(1);
            executorService.scheduleWithFixedDelay(this::sync, cleanUpPeriodSecond, cleanUpPeriodSecond, TimeUnit.SECONDS);
//            Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        }

    }

    /**
     * 加载文件夹
     *
     * @param mapDirPath 文件夹
     * @return 文件夹
     * @throws IOException IO异常
     */
    private File loadDir(String mapDirPath) throws IOException {
        File folder = new File(mapDirPath);
        if (!folder.exists()) {
            folder.mkdirs();
        }
        if (!folder.isDirectory()) {
            throw new IOException("the same name [" + mapDirPath + "] file is exists .");
        }
        return folder;
    }

    /**
     * 加载数据文件
     *
     * @param dir 文件夹
     * @throws IOException IO异常
     */
    private void loadDataFiles(File dir) throws IOException {
        File[] fileList = dir.listFiles((dir1, name) -> name.endsWith(FileMapData.END_FILE_NAME));
        if (fileList == null || fileList.length <= 0) {
            return;
        }
        Arrays.sort(fileList, (o1, o2) -> {
            try {
                int index1 = Integer.parseInt(o1.getName().replace(FileMapData.END_FILE_NAME, ""));
                int index2 = Integer.parseInt(o2.getName().replace(FileMapData.END_FILE_NAME, ""));
                return index1 > index2 ? 1 : -1;
            } catch (Exception e) {
                return 0;
            }
        });
        for (File file : fileList) {
            int index;
            try {
                index = Integer.parseInt(file.getName().replace(FileMapData.END_FILE_NAME, ""));
            } catch (Exception e) {
                continue;
            }
            FileMapData data = new FileMapData(mapDirPath, oneDataFileSize, index);
            datas.put(index, data);
        }
    }

    /**
     * 同步磁盘逻辑
     */
    public void sync() {
        if (isClose) {
            return;
        }
        Map<Integer, Integer> files = info.getFileCounts();
        List<Integer> deletes = new ArrayList<>();
        for (Integer entry : datas.keySet()) {
            if (!files.containsKey(entry)) {
                deletes.add(entry);
            } else {
                if (files.get(entry) <= 0 && entry != info.getNowFileIndex()) {
                    deletes.add(entry);
                }
            }
        }
        if (isClose) {
            return;
        }
        for (Integer in : deletes) {
            FileMapData data = datas.get(in);
            data.delete();
            synchronized (this) {
                datas.remove(in);
                info.removeFileCount(in);
            }
        }
        if (isClose) {
            return;
        }
        hash.sync();
        info.sync();
        for (FileMapData fmd : datas.values()) {
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
//        sync();
        if (executorService != null) {
            executorService.shutdown();
        }
        hash.close();
        info.close();
        for (FileMapData fmd : datas.values()) {
            fmd.close();
        }
        isClose = true;
    }

    /**
     * 删除
     */
    public synchronized void delete() {
        this.close();
        hash.delete();
        info.delete();
        for (FileMapData fmd : datas.values()) {
            fmd.delete();
        }
        hash = null;
        info = null;
        datas.clear();
        new File(mapDirPath).delete();
    }

    /**
     * 元素个数
     */
    public int size() {
        return info.getCount();
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
     * 新建数据文件
     *
     * @return 数据文件
     * @throws IOException IO异常
     */
    private synchronized FileMapData createFileMapData() throws IOException {
        info.addFile();
        FileMapData fmd = new FileMapData(mapDirPath, oneDataFileSize, info.getNowFileIndex());
        datas.put(info.getNowFileIndex(), fmd);
        return fmd;
    }

    /**
     * 获取值
     *
     * @param key 键
     * @return 值
     */
    public String get(String key) {
        byte[] value = getBytes(key);
        if (value == null) {
            return null;
        }
        return new String(value, StandardCharsets.UTF_8);
    }

    /**
     * 获取值
     *
     * @param key 键
     * @return 值
     */
    public byte[] getBytes(String key) {
        if (key == null) {
            return null;
        }
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        return getBytes(bytes);
    }

    /**
     * 获取值
     *
     * @param key 键
     * @return 值
     */
    private byte[] getBytes(byte[] key) {
        return readValue(key, true);
    }

    /**
     * 是否存在
     *
     * @param key 键
     * @return 是否存在
     */
    public boolean exists(String key) {
        if (key == null) {
            return false;
        }
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        return exists(bytes);
    }

    /**
     * 是否存在
     *
     * @param key 键
     * @return 是否存在
     */
    public boolean exists(byte[] key) {
        return readValue(key, false) != null;
    }

    /**
     * 获取值
     *
     * @param key 键
     * @return 值
     */
    private byte[] readValue(byte[] key, boolean readValue) {
        assertFileClosed();
        if (key == null) {
            return null;
        }
        int[] ints = hash.readKeyCoordinate(key);
        if (ints[0] == 0) {
            return null;
        }
        FileMapData fmd = datas.get(ints[1]);
        if (fmd == null) {
            return null;
        }
        byte[] value = null;
        int nowByteIndex = ints[2];
        while (true) {
            MapKeyData data = fmd.readKey(nowByteIndex);
            if (data == null) {
                return null;
            }
            if (Arrays.equals(key, data.getKey())) {
                value = readValue ? fmd.readValue(data.getValuePosition(), data.getValueLength()) : new byte[0];
                break;
            }
            if (!data.isHasNext()) {
                break;
            }
            fmd = datas.get(data.getNextFilePosition());
            if (fmd == null) {
                break;
            }
            nowByteIndex = data.getNextBytePosition();
        }
        return value;
    }

    /**
     * 放入值
     *
     * @param key   键
     * @param value 值
     */
    public synchronized void put(String key, String value) {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        if (value == null || value.length() == 0) {
            return;
        }
        byte[] vbytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] kbytes = key.getBytes(StandardCharsets.UTF_8);
        putBytes(kbytes, vbytes);
    }

    /**
     * 放入值
     *
     * @param key   键
     * @param value 值
     */
    public synchronized void putBytes(String key, byte[] value) {
        if (key == null) {
            throw new IllegalArgumentException("key is null");
        }
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        putBytes(bytes, value);
    }

    /**
     * 放入值
     *
     * @param key   键
     * @param value 值
     */
    public synchronized void putBytes(byte[] key, byte[] value) {
        assertFileClosed();
        if (value == null || value.length == 0) {
            return;
        }
        if (key == null || key.length == 0) {
            throw new IllegalArgumentException("key is null");
        }
        int[] ints = hash.readKeyCoordinate(key);
        try {
            int[] wins = writeMapData(key, value);
            int byteindex = wins[0];
            int length = wins[1];
            if (ints[0] == 0) {
                // 与hash关联
                hash.writeKeyCoordinate(key, info.getNowFileIndex(), byteindex);
            } else {
                int nowFileIn = ints[1];
                int nowByteIn = ints[2];
                FileMapData parentfmd = null;
                int parentByteIn = -1;
                while (true) {
                    FileMapData fmd = datas.get(nowFileIn);
                    MapKeyData data = fmd.readKey(nowByteIn);
                    if (data == null) {
                        // 与hash关联
                        hash.writeKeyCoordinate(key, info.getNowFileIndex(), byteindex);
                        break;
                    }
                    if (Arrays.equals(key, data.getKey())) {
                        // 新节点写入下一个节点位置
                        if (data.isHasNext()) {
                            fmd = datas.get(info.getNowFileIndex());
                            fmd.writeNextPosition(byteindex, data.getNextFilePosition(), data.getNextBytePosition());
                        }
                        // 上一节点写入新节点位置
                        if (parentfmd == null) {
                            hash.writeKeyCoordinate(key, info.getNowFileIndex(), byteindex);
                        } else {
                            parentfmd.writeNextPosition(parentByteIn, info.getNowFileIndex(), byteindex);
                        }
                        info.remove(nowFileIn);
                        break;
                    }
                    if (!data.isHasNext()) {
                        // 与当前节点关联
                        fmd.writeNextPosition(nowByteIn, info.getNowFileIndex(), byteindex);
                        break;
                    }
                    parentfmd = fmd;
                    parentByteIn = nowByteIn;
                    nowFileIn = data.getNextFilePosition();
                    nowByteIn = data.getNextBytePosition();
                }
            }
            info.add(length);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * 写数据
     *
     * @param key   键
     * @param value 值
     * @return 坐标
     * @throws IOException IO异常
     */
    private int[] writeMapData(byte[] key, byte[] value) throws IOException {
        int nowByteIndex = info.getNowByteIndex();
        FileMapData fmd = datas.get(info.getNowFileIndex());
        if (fmd == null) {
            createFileMapData();
            return writeMapData(key, value);
        }
        try {
            int totallength = fmd.writeData(nowByteIndex, key, value);
            return new int[]{nowByteIndex, totallength};
        } catch (FileStoreOutSizeException e1) {
            createFileMapData();
            return writeMapData(key, value);
        }
    }

    /**
     * 删除值
     *
     * @param key 键
     */
    public synchronized void remove(String key) {
        if (key == null || key.length() == 0) {
            return;
        }
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        removeBytes(bytes);
    }

    /**
     * 删除值
     *
     * @param key 键
     */
    public synchronized void removeBytes(byte[] key) {
        assertFileClosed();
        if (key == null || key.length == 0) {
            return;
        }
        int[] ints = hash.readKeyCoordinate(key);
        if (ints[0] == 0) {
            return;
        }
        int nowFileIn = ints[1];
        int nowByteIn = ints[2];
        FileMapData parentfmd = null;
        int parentByteIn = -1;
        while (true) {
            FileMapData fmd = datas.get(nowFileIn);
            MapKeyData data = fmd.readKey(nowByteIn);
            if (data == null) {
                break;
            }
            if (Arrays.equals(key, data.getKey())) {
                if (data.isHasNext()) {
                    // 如果有：将此节点的下一个节点关联，提到此节点的上一个节点上
                    if (parentfmd == null) {
                        hash.writeKeyCoordinate(key, data.getNextFilePosition(), data.getNextBytePosition());
                    } else {
                        parentfmd.writeNextPosition(parentByteIn, data.getNextFilePosition(), data.getNextBytePosition());
                    }
                } else {
                    // 如果没有：将此节点的上一个节点的节点关联制空
                    if (parentfmd == null) {
                        hash.removeKeyCoordinate(key);
                    } else {
                        parentfmd.removeNextPosition(parentByteIn);
                    }
                }
                info.remove(nowFileIn);
                break;
            }
            if (!data.isHasNext()) {
                break;
            }
            parentfmd = fmd;
            parentByteIn = nowByteIn;
            nowFileIn = data.getNextFilePosition();
            nowByteIn = data.getNextBytePosition();
        }
    }

}
