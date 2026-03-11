package com.example.build_your_own_kafka.broker;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Client for interacting with ZooKeeper
 */
@Slf4j
@Component
public class ZookeeperClient implements Watcher {

    private static final int SESSION_TIMEOUT = 30000;

    @Value("${zookeeper.host}")
    private String host;

    @Value("${zookeeper.port}")
    private int port;

    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);


    @PostConstruct
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
        connectedSignal.await();

        // Create required paths if they don't exist
        createPath("/brokers");
        createPath("/topics");
        createPath("/controller");
    }


    @PreDestroy
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
            log.info("ZooKeeper connection closed");
        }
    }


    public String getConnectString() {
        return host + ":" + port;
    }

    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("Created persistent node: {}", path);
        } else {
            zooKeeper.setData(path, data.getBytes(), -1);
            log.info("Updated persistent node: {}", path);
        }
    }


    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            log.info("Created ephemeral node: {}", path);
            return true;
        } else {
            log.info("Ephemeral node already exists: {}", path);
            return false;
        }
    }


    public boolean exists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        return stat != null;
    }


    public String getData(String path) throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data);
    }


    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data.getBytes(), -1);
    }


    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
    }


    private void createPath(String path) {
        try {
            if (path.equals("/")) return;

            int lastSlashIndex = path.lastIndexOf('/');
            if (lastSlashIndex > 0) {
                createPath(path.substring(0, lastSlashIndex));
            }

            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.info("Created ZooKeeper path: {}", path);
            }
        } catch (Exception e) {
            log.warn("Failed to create path: {}", path, e);
        }
    }


    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        List<String> newChildren = zooKeeper.getChildren(path, event2 -> {
                            if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                watchChildren(path, callback);
                            }
                        });
                        callback.onChildrenChanged(newChildren);
                    } catch (Exception e) {
                        log.error("Error processing children changed event", e);
                    }
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            log.error("Failed to watch children for path: {}", path, e);
        }
    }


    public void watchNode(String path, NodeCallback callback) {
        try {
            zooKeeper.exists(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted ||
                        event.getType() == Watcher.Event.EventType.NodeDataChanged ||
                        event.getType() == Watcher.Event.EventType.NodeCreated) {
                    callback.onNodeChanged();
                }
            });
        } catch (Exception e) {
            log.error("Failed to watch node: {}", path, e);
        }
    }


    public void deleteNode(String path) throws KeeperException, InterruptedException {
        if (exists(path)) {
            zooKeeper.delete(path, -1);
            log.info("Deleted node: {}", path);
        }
    }


    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            log.info("Connected to ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            log.warn("Disconnected from ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            log.warn("ZooKeeper session expired, reconnecting...");
            try {
                if (zooKeeper != null) zooKeeper.close();
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                log.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e) {
                log.error("Failed to reconnect to ZooKeeper", e);
            }
        }
    }


    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }


    public interface NodeCallback {
        void onNodeChanged();
    }
}
