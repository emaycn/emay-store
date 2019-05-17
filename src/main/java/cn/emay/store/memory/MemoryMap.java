package cn.emay.store.memory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存KV存储
 * 
 * @author 东旭
 *
 */
public class MemoryMap {

	/**
	 * 清理缓存时间间隔,如果为0则不清理,默认5分钟清理一次
	 */
	private int clearSecond = 300;

	/**
	 * 虚拟缓存
	 */
	private Map<String, Object> cache = new ConcurrentHashMap<String, Object>();

	/**
	 * 过期时间
	 */
	private Map<String, Long> tasks = new ConcurrentHashMap<String, Long>();

	/**
	 * 清理线程
	 */
	private Thread runner;

	/**
	 * 清理线程是否启动
	 */
	private boolean isStart = false;

	/**
	 * 清理内存时间间隔
	 * 
	 * @param clearSecond
	 */
	public MemoryMap(int clearSecond) {
		if (clearSecond > 0) {
			this.clearSecond = clearSecond;
		}
		this.init();
	}

	public MemoryMap() {
		this(300);
	}

	/**
	 * 清理
	 */
	private void init() {
		isStart = true;
		runner = new Thread("MemoryKVStore Clear Thread") {

			private void sleepOnStep(Object waitObj, long insleepTime, long onceSleepTime) {
				long sleepTime = insleepTime;
				while (isStart && sleepTime > 0) {
					long realSleepTime = 0L;
					if (sleepTime > onceSleepTime) {
						realSleepTime = onceSleepTime;
						sleepTime -= onceSleepTime;
					} else {
						realSleepTime = sleepTime;
						sleepTime = 0;
					}
					try {
						waitObj.wait(realSleepTime);
					} catch (InterruptedException e) {
						break;
					}
				}
			}

			@Override
			public synchronized void run() {
				while (isStart) {
					Object[] keys = tasks.keySet().toArray();
					for (Object key : keys) {
						Long timeout = tasks.get((String) key);
						if (timeout != null && timeout <= System.currentTimeMillis()) {
							del((String) key);
						}
					}
					sleepOnStep(this, clearSecond * 1000L, 5000L);
				}
			}
		};
		runner.setDaemon(true);
		runner.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				isStart = false;
			}
		});
	}

	/**
	 * 是否存在
	 * 
	 * @param key
	 * @return
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
	 * @param key
	 */
	public synchronized void del(String key) {
		this.cache.remove(key);
		this.tasks.remove(key);
	}

	/**
	 * 放入
	 * 
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @param expireSecond
	 *            超时时间
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
	 * @param key
	 * @param clazz
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public synchronized <T> T get(String key, Class<T> clazz) {
		Object result = get(key);
		return result == null ? null : (T) result;
	}

	/**
	 * 获取值
	 * 
	 * @param key
	 * @return
	 */
	public synchronized Object get(String key) {
		Long timeout = this.tasks.get((Object) key);
		if (timeout != null && timeout <= System.currentTimeMillis()) {
			return null;
		}
		return this.cache.get(key);
	}

}
