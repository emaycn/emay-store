package cn.emay.store.memory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 内存队列管理
 *
 * @author Frank
 */
public class MemoryQueue {

    /**
     * 锁
     */
    private final Object lock = new Object();

    /**
     * 队列集合
     */
    private final Map<String, Queue<Object>> queues = new ConcurrentHashMap<>();

    /**
     * 是否存在
     *
     * @param queueName 队列名
     * @return 是否存在
     */
    public boolean exists(String queueName) {
        return queues.containsKey(queueName);
    }

    /**
     * 删除队列
     *
     * @param queueName 队列名
     * @return 是否成功
     */
    public boolean del(String queueName) {
        queues.remove(queueName);
        return true;
    }

    /**
     * 往队列中放入数据
     *
     * @param queueName 队列名
     * @param object    数据
     * @return 是否成功
     */
    public boolean offer(String queueName, Object object) {
        Queue<Object> queue = queues.get(queueName);
        if (queue == null) {
            synchronized (lock) {
                queue = queues.get(queueName);
                if (queue == null) {
                    queue = new ConcurrentLinkedQueue<>();
                    queues.put(queueName, queue);
                }
            }
        }
        return queue.offer(object);
    }

    /**
     * 从队列中拿出数据
     *
     * @param queueName 队列名
     * @param clazz     类型
     * @return 数据
     */
    @SuppressWarnings("unchecked")
    public <T> T poll(String queueName, Class<T> clazz) {
        Object obj = poll(queueName);
        if (obj == null) {
            return null;
        }
        return (T) obj;
    }

    /**
     * 从队列中拿出数据
     *
     * @param queueName 队列名
     * @return 数据
     */
    public Object poll(String queueName) {
        Queue<Object> queue = queues.get(queueName);
        if (queue == null) {
            return null;
        }
        return queue.poll();
    }

    /**
     * 队列大小
     *
     * @param queueName 队列名
     * @return 队列大小
     */
    public int size(String queueName) {
        Queue<Object> queue = queues.get(queueName);
        if (queue == null) {
            return 0;
        } else {
            return queue.size();
        }
    }

}
