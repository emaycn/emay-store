package cn.emay.store.file;

import java.io.File;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cn.emay.store.file.queue.FileQueue;
import cn.emay.store.file.queue.FileQueueHistoryReader;
import cn.emay.store.file.queue.HistoryDataHandler;

public class TestQueue {

	private FileQueue queue;

	@Before
	public void pre() {
		queue = new FileQueue("E:\\emaytest\\filequeue", 5, 12 * 1024 * 1024, 6);
	}

	@After
	public void after(){
		queue.close();
		queue.delete();
	}

	@Test
	public void testMap() throws  InterruptedException {
		String value0 = "这个是VALUE，这个是VALUE，这个是VALUE，这个是VALUE，这个是VALUE";

		final int total = 10000 * 100;
		long time = System.currentTimeMillis();

		/*
		 * 测试offer
		 */
		for (int i = 0; i < total; i++) {
			String value = value0 + i;
			queue.offer(value);
		}
		System.out.println("测试offer\t" + (System.currentTimeMillis() - time));
		Assert.assertEquals(queue.size(), total);

		/*
		 * 测试poll
		 */
		time = System.currentTimeMillis();
		for (int i = 0; i < total; i++) {
			String value = value0 + i;
			Assert.assertEquals(queue.poll(), value);
		}
		Assert.assertEquals(queue.size(), 0);
		System.out.println("测试poll\t" + (System.currentTimeMillis() - time));

		Thread.sleep(7l * 1000l);
		
//		testHistory();

	}

	public void testHistory() throws InterruptedException {
		List<File> files = queue.getOldDataFiles();
		System.out.println("old file size :" + files);

		HistoryDataHandler hdh = new HistoryDataHandler() {

			@Override
			public void handle(String data) {
				System.out.println(data);
			}

		};

		for (File file : files) {
			FileQueueHistoryReader.readHistory(file, hdh);
			System.out.println("file:" + file.getName());
		}

		Thread.sleep(6l * 1000l);
	}

}
