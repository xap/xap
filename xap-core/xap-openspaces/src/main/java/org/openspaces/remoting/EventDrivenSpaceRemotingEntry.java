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


package org.openspaces.remoting;

import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.client.IReplicatable;
import com.j_spaces.core.client.MetaDataEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Default implementation of a remoting entry that acts both as a remote invocation and a remote
 * result.
 *
 * @author kimchy
 * @deprecated
 */
@Deprecated
public class EventDrivenSpaceRemotingEntry extends MetaDataEntry implements SpaceRemotingEntry, SmartExternalizable, IReplicatable {

    static final long serialVersionUID = 7009426586658014410L;
    static int bitIndexCounter = 0;
    private static final int LOOKUP_NAME_BIT_MASK = 1 << bitIndexCounter++;
    private static final int METHOD_NAME_BIT_MASK = 1 << bitIndexCounter++;
    private static final int ROUTING_BIT_MASK = 1 << bitIndexCounter++;
    private static final int ONE_WAY_BIT_MASK = 1 << bitIndexCounter++;
    private static final int ARGUMENTS_BIT_MASK = 1 << bitIndexCounter++;
    private static final int META_ARGUMENTS_BIT_MASK = 1 << bitIndexCounter++;
    private static final int RESULT_BIT_MASK = 1 << bitIndexCounter++;
    private static final int EX_BIT_MASK = 1 << bitIndexCounter++;
    private static final int INSTANCE_ID_BIT_MASK = 1 << bitIndexCounter++;


    public Boolean isInvocation;

    public String lookupName;

    public String methodName;

    public Object[] arguments;

    public Object[] metaArguments;

    public Boolean oneWay;

    public Integer routing;

    public Object result;

    public Throwable ex;

    public Integer instanceId;

    /**
     * Constructs a new Async remoting entry. By default a transient one witn that does not return a
     * lease. Also, by default, this is an invocation entry.
     */
    public EventDrivenSpaceRemotingEntry() {
        setNOWriteLeaseMode(true);
        makeTransient();
        setInvocation(Boolean.TRUE);
    }

    public void setInvocation(Boolean invocation) {
        this.isInvocation = invocation;
    }

    public String getLookupName() {
        return lookupName;
    }

    public void setLookupName(String lookupName) {
        this.lookupName = lookupName;
    }

    public String getMethodName() {
        return methodName;
    }

    protected void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Object[] getArguments() {
        return arguments;
    }

    protected void setArguments(Object[] arguments) {
        this.arguments = arguments;
    }

    public Object[] getMetaArguments() {
        return metaArguments;
    }

    public void setMetaArguments(Object[] metaArguments) {
        this.metaArguments = metaArguments;
    }

    public Boolean getOneWay() {
        return oneWay;
    }

    public void setOneWay(Boolean oneWay) {
        this.oneWay = oneWay;
    }

    public Integer getRouting() {
        return routing;
    }

    public void setRouting(Object routing) {
        this.routing = routing.hashCode();
    }

    public Object getResult() {
        return this.result;
    }

    protected void setResult(Object result) {
        this.result = result;
    }

    public Throwable getException() {
        return this.ex;
    }

    protected void setException(Throwable exception) {
        this.ex = exception;
    }

    public Integer getInstanceId() {
        return this.instanceId;
    }

    public void setInstanceId(Integer instanceId) {
        this.instanceId = instanceId;
    }

    public static String[] __getSpaceIndexedFields() {
        return new String[]{"routing"};
    }


    public SpaceRemotingEntry buildInvocation(String lookupName, String methodName, Object[] arguments) {
        clearResultData();
        setInvocation(Boolean.TRUE);
        setLookupName(lookupName);
        setMethodName(methodName);
        setArguments(arguments);
        return this;
    }

    public SpaceRemotingEntry buildResultTemplate() {
        clearInvocationData();
        clearResultData();
        buildResultUID();
        setInvocation(Boolean.FALSE);
        return this;
    }

    public SpaceRemotingEntry buildResult(Throwable e) {
        clearInvocationData();
        buildResultUID();
        setInvocation(Boolean.FALSE);
        setException(e);
        return this;
    }

    public SpaceRemotingEntry buildResult(Object result) {
        clearInvocationData();
        buildResultUID();
        setInvocation(Boolean.FALSE);
        setResult(result);
        return this;
    }

    private void clearResultData() {
        setResult(null);
        setException(null);
    }

    private void clearInvocationData() {
        setLookupName(null);
        setMethodName(null);
        setArguments(null);
        setMetaArguments(null);
        setOneWay(null);
    }

    private void buildResultUID() {
        if (__getEntryInfo() != null && __getEntryInfo().m_UID != null) {
            __getEntryInfo().m_UID += "Result";
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super._writeExternal(out);
        short nullableFieldsBitMask = getNullableFieldsBitMask();
        out.writeShort(nullableFieldsBitMask);
        out.writeBoolean(isInvocation);

        if (isInvocation) {
            if (lookupName != null) {
                out.writeUTF(lookupName);
            }

            if (methodName != null) {
                out.writeUTF(methodName);
            }

            if (routing != null) {
                out.writeInt(routing);
            }

            if (oneWay != null && oneWay) {
                out.writeBoolean(true);
            }

            if (arguments != null && arguments.length != 0) {
                out.writeInt(arguments.length);
                for (Object argument : arguments) {
                    out.writeObject(argument);
                }
            }
            if (metaArguments != null && metaArguments.length != 0) {
                out.writeInt(metaArguments.length);
                for (Object argument : metaArguments) {
                    out.writeObject(argument);
                }
            }
        } else {
            if (result != null) {
                out.writeObject(result);
            }
            if (ex != null) {
                out.writeObject(ex);
            }
            if (routing != null) {
                out.writeInt(routing);
            }
            if (instanceId != null) {
                out.writeInt(instanceId);
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super._readExternal(in);
        short bitMask = in.readShort();
        isInvocation = in.readBoolean();
        if (isInvocation) {
            if (!isFieldNull(bitMask, LOOKUP_NAME_BIT_MASK)) {
                lookupName = in.readUTF();
            }
            if (!isFieldNull(bitMask, METHOD_NAME_BIT_MASK)) {
                methodName = in.readUTF();
            }
            if (!isFieldNull(bitMask, ROUTING_BIT_MASK)) {
                routing = in.readInt();
            }
            if (!isFieldNull(bitMask, ONE_WAY_BIT_MASK)) {
                oneWay = in.readBoolean();
            }

            if (!isFieldNull(bitMask, ARGUMENTS_BIT_MASK)) {
                int argumentNumber = in.readInt();
                arguments = new Object[argumentNumber];
                for (int i = 0; i < argumentNumber; i++) {
                    arguments[i] = in.readObject();
                }
            }

            if (!isFieldNull(bitMask, META_ARGUMENTS_BIT_MASK)) {
                int argumentNumber = in.readInt();
                metaArguments = new Object[argumentNumber];
                for (int i = 0; i < argumentNumber; i++) {
                    metaArguments[i] = in.readObject();
                }
            }
        } else {
            if (!isFieldNull(bitMask, RESULT_BIT_MASK)) {
                result = in.readObject();
            }
            if (!isFieldNull(bitMask, EX_BIT_MASK)) {
                ex = (Throwable) in.readObject();
            }
            if (!isFieldNull(bitMask, ROUTING_BIT_MASK)) {
                routing = in.readInt();
            }
            if (!isFieldNull(bitMask, INSTANCE_ID_BIT_MASK)) {
                instanceId = in.readInt();
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (isInvocation) {
            sb.append("lookupName [").append(lookupName).append("]");
            sb.append(" methodName[").append(methodName).append("]");
            sb.append(" arguments[").append(Arrays.toString(arguments)).append("]");
            sb.append(" metaArguments[").append(Arrays.toString(metaArguments)).append("]");
            sb.append(" routing[").append(routing).append("]");
            sb.append(" oneWay[").append(oneWay).append("]");
        } else {
            if (result != null) {
                sb.append("result[").append(result).append("]");
            }
            if (ex != null) {
                sb.append("ex").append(ex).append("]");
            }
            sb.append(" routing[").append(routing).append("]");
            sb.append(" instanceId[").append(instanceId).append("]");
        }
        return sb.toString();
    }

    /**
     * Returns a bit mask. Fields with non-null values have 1 in their respective index, fields with
     * null values have 0.
     */
    private short getNullableFieldsBitMask() {
        int bitMask = 0;
        bitMask = ((lookupName != null) ? bitMask | LOOKUP_NAME_BIT_MASK : bitMask);
        bitMask = ((methodName != null) ? bitMask | METHOD_NAME_BIT_MASK : bitMask);
        bitMask = ((routing != null) ? bitMask | ROUTING_BIT_MASK : bitMask);
        bitMask = ((oneWay != null) ? bitMask | ONE_WAY_BIT_MASK : bitMask);
        bitMask = ((arguments != null && arguments.length > 0) ? bitMask | ARGUMENTS_BIT_MASK : bitMask);
        bitMask = ((metaArguments != null && metaArguments.length > 0) ? bitMask | META_ARGUMENTS_BIT_MASK : bitMask);
        bitMask = ((result != null) ? bitMask | RESULT_BIT_MASK : bitMask);
        bitMask = ((ex != null) ? bitMask | EX_BIT_MASK : bitMask);
        bitMask = ((instanceId != null) ? bitMask | INSTANCE_ID_BIT_MASK : bitMask);
        return (short) bitMask;
    }

    private boolean isFieldNull(short bitMask, int fieldBitMask) {
        return (bitMask & fieldBitMask) == 0;

    }
}
