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


package com.j_spaces.core.admin;

import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * This class describes the content of the space,
 * on <code>com.j_spaces.core.admin.IRemoteJSpaceAdmin.getRuntimeInfo()</code> call.
 *
 * For example:
 *
 * IJSpace spaceProxy;
 * ...
 * IRemoteJSpaceAdmin spaceAdmin = spaceProxy.getAdmin();
 *
 * SpaceRuntimeInfo rtInfo = spaceAdmin.getRuntimeInfo();
 *
 * String className = rtInfo.m_ClassNames.get(0);
 * Integer count    = rtInfo.m_NumOFEntries.get(0);
 *
 * System.out.println("ClassName: " + className + ", number of entries: " + count);
 *
 * The output should be:
 * ClassName: example.Person, number of entries: 34
 * </pre>
 *
 * @author Igor Goldenberg
 * @version 1.0
 */

public class SpaceRuntimeInfo implements SmartExternalizable {


    private static final long serialVersionUID = -5586653306007649053L;

    /**
     * List of all the classes' names.
     */
    public List<String> m_ClassNames;

    /**
     * List of numbers of entries for each class correlated to <code>m_ClassNames</code>.
     */
    public List<Integer> m_NumOFEntries;//total
    //enhance com.j_spaces.core.admin.SpaceRuntimeInfo with disk num of entries
    public List<Integer> m_RamNumOFEntries;//only ram

    /**
     * List of numbers of pending templates for each class correlated to <code>m_ClassNames</code>.
     */
    public List<Integer> m_NumOFTemplates;

    private long diskSize;

    /**
     * Empty constructor.
     */
    public SpaceRuntimeInfo() {
    }

    /**
     * Constructor.
     *
     * @param classNames   list of names of all the classes in the space
     * @param numOfEntries list of numbers of entries for each class correlated to
     *                     <code>classNames</code>
     */
    public SpaceRuntimeInfo(List<String> classNames, List<Integer> numOfEntries, List<Integer> numOFTemplates, List<Integer> ramNumOFEntries,long diskSize) {
        m_ClassNames = classNames;
        m_NumOFEntries = numOfEntries;
        m_NumOFTemplates = numOFTemplates;
        m_RamNumOFEntries = ramNumOFEntries;
        this.diskSize = diskSize;
    }

    /**
     * {@inheritDoc}
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(m_ClassNames.size());
        for (String name : m_ClassNames) {
            out.writeUTF(name);
        }

        for (Integer num : m_NumOFEntries) {
            out.writeInt(num);
        }

        for (Integer num : m_NumOFTemplates) {
            out.writeInt(num);
        }
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v16_0_0)) {
            for (Integer num : m_RamNumOFEntries) {
                out.writeInt(num);
            }
            out.writeLong(diskSize);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        m_ClassNames = new ArrayList<String>(size);
        m_NumOFEntries = new ArrayList<Integer>(size);
        m_NumOFTemplates = new ArrayList<Integer>(size);
        m_RamNumOFEntries = new ArrayList<Integer>(size);

        for (int i = 0; i < size; ++i) {
            m_ClassNames.add(in.readUTF());
        }

        for (int i = 0; i < size; ++i) {
            m_NumOFEntries.add(in.readInt());
        }

        for (int i = 0; i < size; ++i) {
            m_NumOFTemplates.add(in.readInt());
        }
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v16_0_0)) {
            for (int i = 0; i < size; ++i) {
                m_RamNumOFEntries.add(in.readInt());
            }
            diskSize = in.readLong();
        }
    }

    /**
     * Merge two SpaceRuntimeInfo instances.
     *
     * @return merged SpaceRuntimeInfo instance, actually this
     */
    public SpaceRuntimeInfo appendSpaceRuntimeInfo(SpaceRuntimeInfo spaceRuntimeInfo) {
        if (spaceRuntimeInfo == null) {
            return this;
        }

        List<String> classesNamesList = spaceRuntimeInfo.m_ClassNames;
        int listSize = classesNamesList.size();
        for (int i = 0; i < listSize; i++) {
            String className = classesNamesList.get(i);
            int index = m_ClassNames.indexOf(className);
            if (index >= 0) {
                int newNumEntriesValue =
                        m_NumOFEntries.get(index).intValue() +
                                spaceRuntimeInfo.m_NumOFEntries.get(i).intValue();
                m_NumOFEntries.set(index, newNumEntriesValue);

                int newNumTemplatesValue =
                        m_NumOFTemplates.get(index).intValue() +
                                spaceRuntimeInfo.m_NumOFTemplates.get(i).intValue();
                m_NumOFTemplates.set(index, newNumTemplatesValue);

                int newRamNumEntriesValue =
                        m_RamNumOFEntries.get(index).intValue() +
                                spaceRuntimeInfo.m_RamNumOFEntries.get(i).intValue();
                m_RamNumOFEntries.set(index, newRamNumEntriesValue);

            } else {
                m_ClassNames.add(className);
                m_NumOFEntries.add(spaceRuntimeInfo.m_NumOFEntries.get(i));
                m_NumOFTemplates.add(spaceRuntimeInfo.m_NumOFTemplates.get(i));
                m_RamNumOFEntries.add(spaceRuntimeInfo.m_RamNumOFEntries.get(i));
            }
        }
        diskSize = spaceRuntimeInfo.diskSize;
        return this;
    }

    public long getDiskSize() {
        return diskSize;
    }
}
