package org.apache.zookeeper.recipes.queue;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.util.List;
import java.util.Map;

public class TestInput {

    private ZooKeeper zooKeeper;
    private String dir;
    private List<ACL> acl;
    private byte[] data;
    private List<byte[]> dataList;
    private boolean insert;

    public TestInput(ZooKeeper zooKeeper, String dir, List<ACL> acl, byte[] data, List<byte[]> dataList, boolean insert) {
        this.zooKeeper = zooKeeper;
        this.dir = dir;
        this.acl = acl;
        this.data = data;
        this.dataList = dataList;
        this.insert = insert;
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public String getDir() {
        return dir;
    }

    public List<ACL> getAcl() {
        return acl;
    }

    public byte[] getData() {
        return data;
    }

    public List<byte[]> getDataList() {
        return dataList;
    }

    public boolean isInsert() {
        return insert;
    }
}
