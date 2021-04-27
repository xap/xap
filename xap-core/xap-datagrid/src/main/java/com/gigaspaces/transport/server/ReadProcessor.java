package com.gigaspaces.transport.server;

import com.gigaspaces.transport.NioChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class ReadProcessor {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    public abstract String getName();
    public abstract void read(NioChannel channel) throws IOException;
}
