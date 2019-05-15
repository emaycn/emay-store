# 基于文件、内存的KV存储以及队列

## FileMap

### 说明

 * 文件Map组件
 * 【原理】百万级Key分片，理想情况下积压百万级数据，get操作一次查询;千万级数据，get操作十次查询;亿级数据，get操作百次查询；
 * 【提示】千万级后衰减明显，尽量不要存过多数据；
 * 【建议】超过千万级数据，增加每个数据文件的大小；

### 类

```java

cn.emay.store.file.map.FileMap 

```

	
## FileQueue

### 说明

 * 文件Queue组件
 * 亿级数据文件队列；
 * 数据堆积无性能损耗；
 * 数据消费，不立即删除，可设置消费过的数据保存时效；

### 类

```java

cn.emay.store.file.queue.FileQueue 

```

	
## MemoryMap

### 说明

 * 支持超时删除；
 * 数据全部在内存中；

### 类

```java

cn.emay.store.memory.MemoryMap 

```

	

## MemoryQueue

### 说明

 * 支持多队列；
 * 数据全部在内存中；

### 类

```java

cn.emay.store.memory.MemoryQueue 

```
