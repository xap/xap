package com.gigaspaces.internal.dump;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class HeapReport implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private String heapReport;

    public HeapReport() {
    }

    public HeapReport(String heapReport) {
        this.heapReport = heapReport;
    }

    public HeapReport(HeapReport other) {
        this.heapReport = other.heapReport;
    }


    public String getHeapReport() {
        return heapReport;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(heapReport);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.heapReport = in.readUTF();
    }

    @Override
    public String toString() {
        return "HeapReport{" +
                "HeapReport=" + heapReport+
                '}';
    }
}
