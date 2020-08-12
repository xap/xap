package com.gigaspaces.internal.server.space;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;

public interface ZookeeperClient extends Closeable {

    void addConnectionStateListener(Runnable reconnectTask, ExecutorService executorService);
}
