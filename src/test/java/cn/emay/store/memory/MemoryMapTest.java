package cn.emay.store.memory;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Frank
 */
public class MemoryMapTest {

    @Test
    public void test() {

        MemoryMap store = new MemoryMap(50);

        for (int i = 0; i < 100; i++) {
            store.set("1-" + i, "2-" + i, 5);
        }

        for (int i = 0; i < 100; i++) {
            String v = store.get("1-" + i, String.class);
            Assert.assertEquals(v, "2-" + i);
        }

        for (int i = 0; i < 100; i++) {
            store.del("1-" + i);
        }

        // try {
        // Thread.sleep(6000l);
        // } catch (InterruptedException e) {
        // e.printStackTrace();
        // }

        for (int i = 0; i < 100; i++) {
            String v = store.get("1-" + i, String.class);
            Assert.assertNull(v);
        }

    }

}
