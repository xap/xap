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
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.FileCopyUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * A resource lazy loading script is a lazy loading script that uses Spring abstraction on top of
 * resources on top of a resource. The script resource location uses Spring notation for location
 * (similar to URL, with the addition of <code>classpath:</code> prefix support).
 *
 * <p>When the scipt is constructed, the actual script contents is not loaded. Only if needed the
 * script contents will be loaded. See {@link org.openspaces.remoting.scripting.LazyLoadingRemoteInvocationAspect}.
 *
 * @author kimchy
 */
public class ResourceLazyLoadingScript implements LazyLoadingScript, SmartExternalizable {

    private static final long serialVersionUID = -2086880053176632088L;

    private String name;

    private String type;

    private String script;

    private String resourceLocation;

    private Map<String, Object> parameters;

    private boolean shouldCache = true;


    private Boolean broadcast;

    private RemoteResultReducer remoteResultReducer;

    private Object routing;

    private ResourceLoader resourceLoader = new DefaultResourceLoader();

    private Object[] metaArguments;

    public ResourceLazyLoadingScript() {

    }

    /**
     * Constructs a new lazy loading sctipt.
     *
     * @param name             The script name (used as a unique identifier for cachable scripts).
     * @param type             The type of the script executed
     * @param resoruceLocation The resource location (similar to URL syntax, with additional support
     *                         for <code>classpath:</code> prefix).
     */
    public ResourceLazyLoadingScript(String name, String type, String resoruceLocation) {
        this.name = name;
        this.type = type;
        this.resourceLocation = resoruceLocation;
    }

    /**
     * Returns the scirpt string only if it was already loaded using {@link #loadScript()}.
     */
    public String getScriptAsString() {
        if (script != null) {
            return script;
        }
        throw new ScriptNotLoadedException("Script [" + getName() + "] not loaded");
    }

    /**
     * Returns <code>true</code> if the script has been loaded.
     */
    public boolean hasScript() {
        return script != null;
    }

    /**
     * Loads the scirpt into memory from the resource location.
     */
    public void loadScript() {
        if (script != null) {
            return;
        }
        try {
            script = FileCopyUtils.copyToString(new BufferedReader(new InputStreamReader(resourceLoader.getResource(resourceLocation).getInputStream())));
        } catch (IOException e) {
            throw new ScriptingException("Failed to load script resource [" + resourceLocation + "]", e);
        }
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
     * Returns the routing index (which partition it will "hit").
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
    public ResourceLazyLoadingScript name(String name) {
        this.name = name;
        return this;
    }

    /**
     * Sets the actual script source.
     */
    public ResourceLazyLoadingScript script(String resourceLocation) {
        this.resourceLocation = resourceLocation;
        return this;
    }

    /**
     * Sets the type of the script. For example: <code>groovy</code>.
     */
    public ResourceLazyLoadingScript type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Should this script be cached or not. Deaults to <code>true</code>.
     */
    public ResourceLazyLoadingScript cache(boolean shouldCache) {
        this.shouldCache = shouldCache;
        return this;
    }

    /**
     * Puts a parameter that can be used during script execution.
     *
     * @param name  The name of the parameter.
     * @param value The value of the parameter.
     */
    public ResourceLazyLoadingScript parameter(String name, Object value) {
        if (parameters == null) {
            parameters = new HashMap<String, Object>();
        }
        parameters.put(name, value);
        return this;
    }

    /**
     * Sets the routing index (which partition this will "hit") for the script. Defaults to a random
     * routing. Defaults to a random routing.
     */
    public ResourceLazyLoadingScript routing(Object routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Broadcast the execution of this script over all active partitions. Optionally use a reducer
     * to reduce the results.
     */
    public <T, Y> ResourceLazyLoadingScript broadcast(RemoteResultReducer<T, Y> reducer) {
        this.broadcast = true;
        this.remoteResultReducer = reducer;
        return this;
    }

    /**
     * Adds another meta argument to the script
     *
     * @see org.openspaces.remoting.scripting.Script#getMetaArguments()
     * @see org.openspaces.remoting.SpaceRemotingInvocation#getMetaArguments()
     * @see org.openspaces.remoting.scripting.ScriptingMetaArgumentsHandler
     */
    public ResourceLazyLoadingScript metaArguments(Object... metaArguments) {
        this.metaArguments = metaArguments;
        return this;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(type);
        if (script == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeObject(script);
        }
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
        if (in.readBoolean()) {
            script = (String) in.readObject();
        }
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
        sb.append("ResourceLazyLoadingScript name[").append(getName()).append("]");
        sb.append(" type [").append(getType()).append("]");
        sb.append(" resource location [").append(resourceLocation).append("]");
        sb.append(" script [").append(script).append("]");
        sb.append(" parameters [").append(parameters).append("]");
        return sb.toString();
    }
}
