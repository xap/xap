package com.gigaspaces.transport;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ObjectIntegerMap;
import com.gigaspaces.internal.io.IMarshalOutputStream;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

public class LightMarshalOutputStream extends ObjectOutputStream implements IMarshalOutputStream {
    private static final int CODE_NULL = 0;

    private final Context _context;

    public LightMarshalOutputStream(OutputStream out, Context context) throws IOException {
        super(out);
        this._context = context;
    }

    /**
     * Writes a repetitive object. If this is the first time this object is written, it is assigned
     * a code which is written along the object. The next time the object is written using this
     * method, only its code is written.
     *
     * NOTE: Objects written using this method are cached and are NEVER cleaned up. If the object
     * you are writing is using a PU resource it will not be released when the PU is terminated, and
     * will cause a leak.
     */
    @Override
    public void writeRepetitiveObject(Object obj)
            throws IOException {
        // If object is null, write null code:
        if (obj == null) {
            writeInt(CODE_NULL);
            return;
        }

        // Look for object in cache:
        int code = _context.getObjectCode(obj);

        // If object is cached, write only its code:
        if (code != CODE_NULL) {
            writeInt(code);
        } else {
            // store in cache, get new code:
            code = _context.addObject(obj);
            // Write code and object:
            writeInt(code);
            writeObject(obj);
        }
    }

    public static class Context {
        private final ObjectIntegerMap<Object> _repetitiveObjectsCache = CollectionsFactory.getInstance().createObjectIntegerMap();
        private int _repetitiveObjectCounter = CODE_NULL + 1;

        public int getObjectCode(Object obj) {
            return _repetitiveObjectsCache.get(obj);
        }

        public int addObject(Object obj) {
            int code = _repetitiveObjectCounter++;
            _repetitiveObjectsCache.put(obj, code);
            return code;
        }

        public void close() {
            _repetitiveObjectsCache.clear();
        }
    }
}
