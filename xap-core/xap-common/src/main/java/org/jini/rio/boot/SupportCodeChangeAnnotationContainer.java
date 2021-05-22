package org.jini.rio.boot;

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by tamirs
 * on 11/8/16.
 */
public class SupportCodeChangeAnnotationContainer implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private String version = "";

    public static final SupportCodeChangeAnnotationContainer ONE_TIME = new SupportCodeChangeAnnotationContainer();

    // Externalizable
    public SupportCodeChangeAnnotationContainer() {
    }

    public SupportCodeChangeAnnotationContainer(String version) {
        this.version = version;
    }


    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        BootIOUtils.writeString(out, version);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        version = BootIOUtils.readString(in);
    }

    @Override
    public String toString() {
        return "SupportCodeChangeContainer{" +
                "version='" + version + '\'' +
                '}';
    }

}
