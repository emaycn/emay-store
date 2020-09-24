package cn.emay.store.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 内存KV存储
 *
 * @author 东旭
 */
public class MemoryMap {

    /**
     * 虚拟缓存
     */
    private final Map<String, Object> cache = new ConcurrentHashMap<>();
    /**
     * 过期时间
     */
    private final Map<String, Long> tasks = new ConcurrentHashMap<>();
    /**
     * 清理线程
     */
    private ScheduledExecutorService executorService;

    /**
     * 默认5分钟清理一次
     */
    public MemoryMap() {
        this(300);
    }

    /**
     * @param clearSecond 清理频率,小于等于0则不清理
     */
    public MemoryMap(int clearSecond) {
        if (clearSecond > 0) {
            executorService = Executors.newScheduledThreadPool(1);
            executorService.scheduleWithFixedDelay(() -> {
                List<String> keys = new ArrayList<>(tasks.keySet());
                keys.forEach(key -> {
                    Long timeout = tasks.get(key);
                    if (timeout != null && timeout <= System.currentTimeMillis()) {
                        del(key);
                    }
                });
            }, clearSecond, clearSecond, TimeUnit.SECONDS);
            Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        }
    }

    /**
     * 关闭
     */
    public void close() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    /**
     * 是否存在
     *
     * @param key 键
     * @return 是否存在
     */
    public boolean exists(String key) {
        Long timeout = this.tasks.get(key);
        if (timeout != null && timeout <= System.currentTimeMillis()) {
            return false;
        }
        Object result = this.cache.get(key);
        return result != null;
    }

    /**
     * 删除
     *
     * @param key 键
     */
    public synchronized void del(String key) {
        this.cache.remove(key);
        this.tasks.remove(key);
    }

    /**
     * 放入
     *
     * @param key          键
     * @param value        值
     * @param expireSecond 超时时间
     */
    public synchronized void set(String key, Object value, int expireSecond) {
        this.cache.put(key, value);
        if (expireSecond > 0) {
            this.tasks.put(key, expireSecond * 1000 + System.currentTimeMillis());
        }
    }

    /**
     * 获取值
     *
     * @param key   key
     * @param clazz 类型
     * @return 值
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T get(String key, Class<T> clazz) {
        Object result = get(key);
        return result == null ? null : (T) result;
    }

    /**
     * 获取值
     *
     * @param key 键
     * @return 值
     */
    public synchronized Object get(String key) {
        Long timeout = this.tasks.get(key);
        if (timeout != null && timeout <= System.currentTimeMillis()) {
            return null;
        }
        return this.cache.get(key);
    }

}
