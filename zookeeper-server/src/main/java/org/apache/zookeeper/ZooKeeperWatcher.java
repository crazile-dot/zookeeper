package org.apache.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

/**
 * Zookeeper Wathcher 
 * This class is a Watcher class org.apache.zookeeper.Watcher class)
 * @author(alienware)
 * @since 2015-6-14
 */
public class ZooKeeperWatcher implements Watcher {
    /** Define session expiration time */
    private static final int SESSION_TIMEOUT = 5000;
    /** zookeeper server address */
    private static final String CONNECTION_ADDR = "ip1:port1,ip2:port2,ip3:port3";
    /** zk Parent path settings */
    private static final String PARENT_PATH = "/a";
    /** zk Subpath settings */
    private static final String CHILDREN_PATH = "/b/c";
    /** zk variable */
    private ZooKeeper zk = null;

    /**
     * Create ZK connection
     * @param connectAddr ZK Server address list
     * @param sessionTimeout Session Timeout
     */
    public void createConnection(String connectAddr, int sessionTimeout) {
        this.releaseConnection();
        try {
            //this means to pass the current object to it (that is, the new ZooKeeperWatcher() instance object instantiated in the main function)
            zk = new ZooKeeper(connectAddr, sessionTimeout, this);
            //System.out.println(LOG_PREFIX_OF_MAIN + "Start connection ZK The server");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Close ZK connection
     */
    public void releaseConnection() {
        if (this.zk != null) {
            try {
                this.zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Processing after receiving a Watcher notification from the Server.
     */
    @Override
    public void process(WatchedEvent event) {

        System.out.println("get into process method");

        if (event == null) {
            return;
        }

        // Get connection status
        KeeperState keeperState = event.getState();
        // Event type
        EventType eventType = event.getType();
        // Affected path
        String path = event.getPath();
        System.out.println("Connection status:\t" + keeperState.toString());
        System.out.println("Event type:\t" + eventType.toString());

        if (KeeperState.SyncConnected == keeperState) {
            // Successfully connected to ZK server
            if (EventType.None == eventType) {
                System.out.println( "Successfully connected to ZK The server");
                //connectedSemaphore.countDown();
            }
            //Create node
            else if (EventType.NodeCreated == eventType) {
                System.out.println("Node creation");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //Update node
            else if (EventType.NodeDataChanged == eventType) {
                System.out.println("Node data update");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //Update child nodes
            else if (EventType.NodeChildrenChanged == eventType) {
                System.out.println("Child node change");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //Delete node
            else if (EventType.NodeDeleted == eventType) {
                System.out.println("node " + path + " Deleted");
            }
            else ;
        }
        else if (KeeperState.Disconnected == keeperState) {
            System.out.println("And ZK Server Disconnected");
        }
        else if (KeeperState.AuthFailed == keeperState) {
            System.out.println("Permission check failed");
        }
        else if (KeeperState.Expired == keeperState) {
            System.out.println("Session failure");
        }

    }

}