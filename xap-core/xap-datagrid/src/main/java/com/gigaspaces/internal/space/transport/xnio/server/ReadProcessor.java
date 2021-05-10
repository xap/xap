package com.gigaspaces.internal.space.transport.xnio.server;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.space.transport.xnio.XNioChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public abstract class ReadProcessor {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    public abstract String getName();
    public abstract void read(XNioChannel channel) throws IOException;
}