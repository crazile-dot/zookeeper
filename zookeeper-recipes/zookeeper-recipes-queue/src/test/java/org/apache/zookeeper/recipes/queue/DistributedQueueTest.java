package org.apache.zookeeper.recipes.queue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertNotEquals;

@RunWith(Parameterized.class)
public class DistributedQueueTest {

    private static DistributedQueue distributedQueue;
    private ZooKeeper zooKeeper;
    //per l'istanza zookeeper servono i parametri:
    private String connect;
    private int timeout;
    private Watcher watcher;
    private String dir;
    private List<ACL> acl;
    private byte[] data;
    private List<byte[]> dataList;
    private boolean insert;


    public DistributedQueueTest(TestInput testInput) {
        this.zooKeeper = testInput.getZooKeeper();
        this.dir = testInput.getDir();
        this.acl = testInput.getAcl();
        this.data = testInput.getData();
        this.dataList = testInput.getDataList();
        this.insert = testInput.isInsert();
    }


    @Parameterized.Parameters
    public static Collection<TestInput> configure() throws Exception {
        Collection<TestInput> params = new ArrayList<>();

        ACL acl1 = new ACL();
        ACL acl2 = new ACL();
        ACL acl3 = new ACL();
        ACL acl4 = new ACL();
        ACL acl5 = new ACL();

        List<ACL> aclList = new ArrayList<>();
        aclList.add(acl1);
        aclList.add(acl2);
        aclList.add(acl3);
        aclList.add(acl4);
        aclList.add(acl5);

        byte[] b1 = "string1".getBytes(StandardCharsets.UTF_8);
        byte[] b2 = "string2".getBytes(StandardCharsets.UTF_8);
        byte[] b3 = "string3".getBytes(StandardCharsets.UTF_8);
        byte[] b4 = "string4".getBytes(StandardCharsets.UTF_8);
        byte[] b5 = "string5".getBytes(StandardCharsets.UTF_8);

        List<byte[]> bList = new ArrayList<>();
        bList.add(b1);
        bList.add(b2);
        bList.add(b3);
        bList.add(b4);
        bList.add(b5);

        params.add(new TestInput(new ZooKeeper("connection", 15000, new ZooKeeperWatcher()), "/src/test/utils/directory", aclList, "string1".getBytes(StandardCharsets.UTF_8), bList, true));
        params.add(new TestInput(new ZooKeeper("connection", 15000, new ZooKeeperWatcher()), "/src/test/utils/directory", aclList, "string22".getBytes(StandardCharsets.UTF_8), bList, false));
        params.add(new TestInput(new ZooKeeper("connection", 1000, new ZooKeeperWatcher()), "/src/test/utils/directory", aclList, "string333".getBytes(StandardCharsets.UTF_8), bList, true));
        params.add(new TestInput(new ZooKeeper("connection", 1500000000, new ZooKeeperWatcher()), "/src/test/utils/directory", aclList, "string55555".getBytes(StandardCharsets.UTF_8), bList, false));
        params.add(new TestInput(new ZooKeeper("connection", 0, new ZooKeeperWatcher()), "/src/test/utils/directory", aclList, "string4444".getBytes(StandardCharsets.UTF_8), bList, true));

        return  params;
    }

    @Before
    public void prepare() {
        distributedQueue = new DistributedQueue(this.zooKeeper, this.dir, this.acl);
    }

    @After
    public void f2() {

    }

    @Test
    public void testElement() {
        try {
            // crea coda vuota e inserisci tot elementi (inserimento in coda) e
            // poi controlla se il primo inserito è quello che si trova in testa

            if(distributedQueue.peek() == null) {
                System.out.println("The queue is empty");

                for(int i = 0; i < dataList.size(); i++) {
                    distributedQueue.offer(dataList.get(i));
                }
            }
            assertNotNull(distributedQueue.element());
            assertEquals(dataList.get(0), distributedQueue.element());

        } catch(InterruptedException ie) {
            System.out.println("InterruptedException handled");
        } catch(KeeperException ke) {
            System.out.println("KeeperException handled");
        }

    }

    @Test
    public void testOffer() {
        try{
            for(byte[] elem: this.dataList) {
                assertTrue(distributedQueue.offer(elem));
                assertNotNull(elem);
            }

        } catch(InterruptedException ie) {
            System.out.println("InterruptedException handled");
        } catch(KeeperException ke) {
            System.out.println("KeeperException handled");
        }
    }

    @Test
    public void testRemove() {
        try {
            // crea coda vuota e inserisci tot elementi (inserimento in coda) e
            // poi controlla se il primo inserito è quello che si trova in testa

            if(distributedQueue.peek() == null) {
                System.out.println("The queue is empty");

                for(int i = 0; i < dataList.size(); i++) {
                    distributedQueue.offer(dataList.get(i));
                }
            }
            byte[] data = distributedQueue.remove();
            assertNotNull(data);
            assertEquals(dataList.get(0), data);
            assertNotEquals(data, distributedQueue.element());

        } catch(InterruptedException ie) {
            System.out.println("InterruptedException handled");
        } catch(KeeperException ke) {
            System.out.println("KeeperException handled");
        }
    }

    @Test
    public void testPeek() {
        try {
            assertNull(distributedQueue.peek());
            distributedQueue.offer(this.data);
            assertNotNull(distributedQueue.peek());
            assertEquals(distributedQueue.peek(), this.data);

        } catch(InterruptedException ie) {
            System.out.println("InterruptedException handled");
        } catch(KeeperException ke) {
            System.out.println("KeeperException handled");
        }

    }

    @Test
    public void testPoll() {
        try {
            assertNull(distributedQueue.poll());
            distributedQueue.offer(this.data);
            byte[] data = distributedQueue.poll();
            assertNotNull(data);
            assertEquals(data, this.data);
            assertNotEquals(data, distributedQueue.element());

        } catch (InterruptedException ie) {
            System.out.println("InterruptedException handled");
        } catch (KeeperException ke) {
            System.out.println("KeeperException handled");
        }
    }

}
