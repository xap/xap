/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.openspaces.remoting.scripting;

import com.gigaspaces.serialization.SmartExternalizable;
import org.openspaces.remoting.RemoteResultReducer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * A script that holds the actual script as a String. The name, type, and script must be provided.
 *
 * @author kimchy
 */
public class StaticScript implements Script, SmartExternalizable {

    private static final long serialVersionUID = -5652707951323528455L;

    private String name;
    private String type;
    private String script;
    private Map<String, Object> parameters;
    private boolean shouldCache = true;
    private Object routing;
    private Boolean broadcast;
    private RemoteResultReducer remoteResultReducer;
    private Object[] metaArguments;

    /**
     * Constructs a new static script. Note, the name, type, and script must be provided.
     */
    public StaticScript() {

    }

    /**
     * Constructs a new static script.
     *
     * @param name   The name of the script.
     * @param type   The type of the script (for example, <code>groovy</code>).
     * @param script The actual script as a String.
     */
    public StaticScript(String name, String type, String script) {
        this.name = name;
        this.type = type;
        this.script = script;
    }

    /**
     * Returns the script as a String.
     */
    public String getScriptAsString() {
        return this.script;
    }

    /**
     * Returns the name of the script. Must uniquely identify a script.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns the type of a script. For example: <code>groovy</code>.
     */
    public String getType() {
        return this.type;
    }

    /**
     * Returns the set of parameters that will be passes to the script execution.
     */
    public Map<String, Object> getParameters() {
        return this.parameters;
    }

    /**
     * Returns if this script should be cached or not.
     */
    public boolean shouldCache() {
        return this.shouldCache;
    }

    /**
     * Returns the routing index.
     */
    public Object getRouting() {
        return this.routing;
    }

    public RemoteResultReducer getReducer() {
        return this.remoteResultReducer;
    }

    public Boolean shouldBroadcast() {
        return this.broadcast;
    }

    public Object[] getMetaArguments() {
        return this.metaArguments;
    }

    /**
     * Sets the name of the script.
     */
    public StaticScript name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Sets the actual script source.
     */
    public StaticScript script(String script) {
        this.script = script;
        return this;
    }

    /**
     * Sets the type of the script. For example: <code>groovy</code>.
     */
    public StaticScript type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Should this script be cached or not. Deaults to <code>true</code>.
     */
    public StaticScript cache(boolean shouldCache) {
        this.shouldCache = shouldCache;
        return this;
    }

    /**
     * Puts a parameter that can be used during script execution.
     *
     * @param name  The name of the parameter.
     * @param value The value of the parameter.
     */
    public StaticScript parameter(String name, Object value) {
        if (parameters == null) {
            parameters = new HashMap<String, Object>();
        }
        parameters.put(name, value);
        return this;
    }

    /**
     * Sets the routing index (which partition it will "hit") for the scirpt.
     */
    public StaticScript routing(Object routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Adds another meta argument to the script
     *
     * @see org.openspaces.remoting.scripting.Script#getMetaArguments()
     * @see org.openspaces.remoting.SpaceRemotingInvocation#getMetaArguments()
     * @see org.openspaces.remoting.scripting.ScriptingMetaArgumentsHandler
     */
    public StaticScript metaArguments(Object... metaArguments) {
        this.metaArguments = metaArguments;
        return this;
    }

    /**
     * Broadcast the execution of this script over all active partitions. Optionally use a reducer
     * to reduce the results.
     */
    public <T, Y> StaticScript broadcast(RemoteResultReducer<T, Y> reducer) {
        this.broadcast = true;
        this.remoteResultReducer = reducer;
        return this;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(type);
        out.writeObject(script);
        out.writeBoolean(shouldCache);
        if (parameters == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeShort(parameters.size());
            for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF();
        type = in.readUTF();
        script = (String) in.readObject();
        shouldCache = in.readBoolean();
        if (in.readBoolean()) {
            int size = in.readShort();
            parameters = new HashMap<String, Object>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readUTF();
                Object value = in.readObject();
                parameters.put(key, value);
            }
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("StaticScript name[").append(getName()).append("]");
        sb.append(" type [").append(getType()).append("]");
        sb.append(" script [").append(script).append("]");
        sb.append(" parameters [").append(parameters).append("]");
        return sb.toString();
    }
}
