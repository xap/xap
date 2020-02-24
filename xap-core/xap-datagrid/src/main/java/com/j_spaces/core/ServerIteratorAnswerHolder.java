package com.j_spaces.core;

import com.gigaspaces.internal.server.space.iterator.ServerIteratorInfo;

public class ServerIteratorAnswerHolder extends AnswerHolder {
    private final int batchNumber;

    public ServerIteratorAnswerHolder(int batchNumber) {
        super();
        this.batchNumber = batchNumber;
    }

    public int getBatchNumber() {
        return batchNumber;
    }
}
