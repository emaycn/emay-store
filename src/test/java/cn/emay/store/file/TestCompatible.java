package cn.emay.store.file;

import java.io.IOException;

import cn.emay.store.file.map.FileMap;
import cn.emay.store.file.queue.FileQueue;

/**
 * 拿V1生成的文件，交给V2执行
 * 
 * @author Frank
 *
 */
public class TestCompatible {

	public static void main(String[] args) throws IOException {

		FileQueue queue = new FileQueue("E:\\emaytest\\filequeue");
		FileMap map = new FileMap("E:\\emaytest\\filemap");

		String key0 = "这个是KEY";
		String value0 = "这个是VALUE，这个是VALUE，这个是VALUE";

		int total = 10000 * 100;

		long time = System.currentTimeMillis();

		/*
		 * 测试put
		 */
		for (int i = 0; i < total; i++) {
			String key = key0 + i;
			String value = value0 + i;
			if (!value.equals(map.get(key))) {
				System.out.println(key);
			}
		}

		System.out.println("测试offer\t" + (System.currentTimeMillis() - time));

		/*
		 * 测试offer
		 */
		time = System.currentTimeMillis();
		for (int i = 0; i < total; i++) {
			String value = value0 + i;
			if (!value.equals(queue.poll())) {
				System.out.println(value);
			}
		}
		System.out.println("测试offer\t" + (System.currentTimeMillis() - time));

		queue.close();
		map.close();
	}

}
