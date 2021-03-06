package cn.emay.store.file;

import cn.emay.store.file.map.FileMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Frank
 */
public class FileMapTest {

    private FileMap map;

    @Before
    public void pre() {
        long time;
        map = new FileMap("./emaytest/filemap", 5, 12 * 1024 * 1024, 1024 * 1024);
        time = System.currentTimeMillis();
        System.out.println("load ok\t" + (System.currentTimeMillis() - time));
    }

    @After
    public void after() {
        map.close();
        map.delete();
    }

    @Test
    public void testMap() throws InterruptedException {
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
            map.put(key, value);
        }
        System.out.println("测试put\t" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();
        Assert.assertEquals(map.size(), total);
        for (int i = 0; i < total; i++) {
            String key = key0 + i;
            String value = value0 + i;
            Assert.assertEquals(map.get(key), value);
        }
        System.out.println("测试put ok\t" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();

        /*
         * 测试覆盖
         */
        for (int i = 0; i < total; i++) {
            String key = key0 + i;
            String value = value0 + i + i;
            map.put(key, value);
        }
        System.out.println("测试覆盖\t" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();
        Assert.assertEquals(map.size(), total);
        for (int i = 0; i < total; i++) {
            String key = key0 + i;
            String value = value0 + i + i;
            Assert.assertEquals(map.get(key), value);
        }
        System.out.println("测试覆盖 ok\t" + (System.currentTimeMillis() - time));
        time = System.currentTimeMillis();

        System.out.println("ishas 111" + map.exists(key0 + "111"));

        /*
         * 测试删除
         */
        for (int i = 0; i < total; i++) {
            String key = key0 + i;
            map.remove(key);
        }
        System.out.println("测试删除\t" + (System.currentTimeMillis() - time));
        Assert.assertEquals(map.size(), 0);
        for (int i = 0; i < total; i++) {
            String key = key0 + i;
            Assert.assertNull(map.get(key));
        }
        System.out.println("测试删除 ok\t" + (System.currentTimeMillis() - time));

        System.out.println("ishas 111" + map.exists(key0 + "111"));

        Thread.sleep(6L * 1000L);

    }

}
