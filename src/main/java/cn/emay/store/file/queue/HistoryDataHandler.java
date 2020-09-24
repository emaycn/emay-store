package cn.emay.store.file.queue;

/**
 * 队列数据处理器
 *
 * @author Frank
 */
public interface HistoryDataHandler {

    /**
     * 处理数据
     *
     * @param data 数据
     */
    void handle(String data);

}
