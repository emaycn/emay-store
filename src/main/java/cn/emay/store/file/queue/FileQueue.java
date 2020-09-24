package cn.emay.store.file.queue;

import cn.emay.store.file.exception.FileStoreClosedException;
import cn.emay.store.file.exception.FileStoreOutSizeException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 文件队列<br/>
 *
 * @author Frank
 */
public class FileQueue implements Closeable {

    /**
     * 默认数据文件长度【10m】
     */
    protected final static int DEFAULT_FILE_SIZE = 1024 * 1024 * 10;
    /**
     * 数据文件
     */
    private final Map<Integer, FileQueueData> datas = new ConcurrentHashMap<>();
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
    private final String queueDirPath;
    /**
     * 已经消费的数据，保留时间
     */
    private final long usedDataExpiryMill;
    /**
     * 已经消费完的文件序号
     */
    private final TreeMap<Integer, File> oldList = new TreeMap<>();
    /**
     * 清理线程
     */
    private ScheduledExecutorService executorService;

    /**
     * 构造方法：开启启动检查、10M一个数据文件、30秒刷盘时间间隔、已消费数据不保留
     *
     * @param queueDirPath 队列文件夹位置
     */
    public FileQueue(String queueDirPath) {
        this(queueDirPath, 30, DEFAULT_FILE_SIZE, -1);
    }

    /**
     * @param queueDirPath         队列文件夹位置
     * @param cleanUpPeriodSecond  刷盘时间间隔，秒，小于等于0则不主动落盘
     * @param oneDataFileSize      单数据文件大小【堆积数据越多，单数据请设置越大，不要超过1G】
     * @param usedDataExpirySecond 已经消费的数据，保留时间
     */
    public FileQueue(String queueDirPath, int cleanUpPeriodSecond, int oneDataFileSize, int usedDataExpirySecond) {
        this.queueDirPath = queueDirPath;
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
        if (cleanUpPeriodSecond > 0) {
            executorService = Executors.newScheduledThreadPool(1);
            executorService.scheduleWithFixedDelay(this::sync, cleanUpPeriodSecond, cleanUpPeriodSecond, TimeUnit.SECONDS);
//            Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        }
    }

    /**
     * 加载文件夹
     *
     * @param queueDirPath 文件夹路径
     * @return 文件夹
     * @throws IOException IO异常
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
     * @param dir 文件
     * @throws IOException IO异常
     */
    private void loadDataFiles(File dir) throws IOException {
        File[] fileList = dir.listFiles((dir1, name) -> name.endsWith(FileQueueData.END_FILE_NAME));
        if (fileList == null || fileList.length <= 0) {
            return;
        }
        Arrays.sort(fileList, (o1, o2) -> {
            try {
                int index1 = Integer.parseInt(o1.getName().replace(FileQueueData.END_FILE_NAME, ""));
                int index2 = Integer.parseInt(o2.getName().replace(FileQueueData.END_FILE_NAME, ""));
                return index1 > index2 ? 1 : -1;
            } catch (Exception e) {
                return 0;
            }
        });
        for (File file : fileList) {
            int index;
            try {
                index = Integer.parseInt(file.getName().replace(FileQueueData.END_FILE_NAME, ""));
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
     * 同步逻辑
     */
    public void sync() {
        if (isClose) {
            return;
        }
        Map<Integer, File> olds = new HashMap<>();
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

        List<Integer> deleteList = new ArrayList<>();
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
//        sync();
        if (executorService != null) {
            executorService.shutdown();
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
     * @return 数据文件
     * @throws IOException IO异常
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
     * @return 队列大小
     */
    public int size() {
        return info.getCount();
    }

    /**
     * 压入数据
     *
     * @param value 数据
     */
    public synchronized void offer(String value) {
        if (value == null) {
            return;
        }
        byte[] bytes;
        bytes = value.getBytes(StandardCharsets.UTF_8);
        offerBytes(bytes);
    }

    /**
     * 压入数据
     *
     * @param value 数据
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
                createFileQueueData();
            } catch (IOException e1) {
                throw new IllegalArgumentException(e1);
            }
            offerBytes(value);
        }
    }

    /**
     * 弹出数据
     *
     * @return 数据
     */
    public synchronized String poll() {
        byte[] bytes = pollBytes();
        if (bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 弹出数据
     *
     * @return 数据
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
     * @return 历史数据文件
     */
    public List<File> getOldDataFiles() {
        assertFileClosed();
        return new ArrayList<>(oldList.values());
    }

}
