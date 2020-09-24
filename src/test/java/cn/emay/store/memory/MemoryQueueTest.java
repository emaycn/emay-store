package cn.emay.store.memory;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Frank
 */
public class MemoryQueueTest {

    @Test
    public void test() {

        MemoryQueue store = new MemoryQueue();

        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10000; j++) {
                store.offer(i + "-queue", i + "-" + j + "-data");
            }
        }

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(store.size(i + "-queue"), 10000);
        }

        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10000; j++) {
                String data = store.poll(i + "-queue", String.class);
                Assert.assertEquals(data, i + "-" + j + "-data");
            }
        }

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(store.size(i + "-queue"), 0);
            String data = store.poll(i + "-queue", String.class);
            Assert.assertNull(data);
        }

    }

}
