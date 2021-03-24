package com.gigaspaces.proxy_pool;

import com.j_spaces.jdbc.driver.GConnection;

public class BusyValue {
        int refCount;
        GConnection connection;

    @Override
    public String toString() {
        return "BusyValue{" +
                " refCount=" + refCount +
                ", connection=" + connection +
                '}';
    }

    public BusyValue() {
        }

        public BusyValue(int refCount, GConnection connection) {
            this.refCount = refCount;
            this.connection = connection;
        }

        public int getRefCount() {
            return refCount;
        }


    public void setRefCount(int refCount) {
        this.refCount = refCount;
    }

    public void setConnection(GConnection connection) {
        this.connection = connection;
    }

    public GConnection getConnection() {
            return connection;
        }


    }


