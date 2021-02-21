package com.gigaspaces.internal.io;

import java.io.IOException;

public interface IMarshalInputStream {
    Object readRepetitiveObject() throws IOException, ClassNotFoundException;
}
