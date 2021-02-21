package com.gigaspaces.transport;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerObjectMap;
import com.gigaspaces.internal.io.IMarshalInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class LightMarshalInputStream extends ObjectInputStream implements IMarshalInputStream {
    private final Context _context;
    private static final int CODE_NULL = 0;

    public LightMarshalInputStream(InputStream in, Context context) throws IOException {
        super(in);
        this._context = context;
    }

    @Override
    public Object readRepetitiveObject() throws IOException, ClassNotFoundException {
        // Read object code:
        int code = readInt();
        // If null return:
        if (code == CODE_NULL)
            return null;

        // Look for cached object by code:
        Object value = _context._repetitiveObjectsCache.get(code);

        if (value != null)
            return value;

        // If object is not cached, read it from the stream and cache it:
        value = readObject();
        // If object is string, intern it:
        if (value instanceof String)
            value = ((String) value).intern();
        _context._repetitiveObjectsCache.put(code, value);

        return value;
    }

    public static class Context {
        private final IntegerObjectMap<Object> _repetitiveObjectsCache = CollectionsFactory.getInstance().createIntegerObjectMap();

        public void close() {
            _repetitiveObjectsCache.clear();
        }
    }
}
