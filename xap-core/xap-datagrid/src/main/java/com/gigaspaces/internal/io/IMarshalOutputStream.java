package com.gigaspaces.internal.io;

import java.io.IOException;

public interface IMarshalOutputStream {
    void writeRepetitiveObject(Object obj) throws IOException;
}
