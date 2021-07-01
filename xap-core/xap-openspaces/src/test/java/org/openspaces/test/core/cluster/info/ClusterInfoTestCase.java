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
package org.openspaces.test.core.cluster.info;

import org.junit.Assert;
import org.junit.Test;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoBuilder;
import org.openspaces.core.cluster.internal.ClusterInfoImpl;

public class ClusterInfoTestCase {
    @Test
    public void testClusterInfoCopy() {
        ClusterInfo clusterInfo = createClusterInfo(0);
        ClusterInfo copy = clusterInfo.copy();
        assertEquals(clusterInfo, copy);
        Assert.assertNotSame(clusterInfo, copy);
        Assert.assertFalse(clusterInfo instanceof ClusterInfoImpl);

        copy.setSchema("schema-copy");
        Assert.assertEquals("schema-copy", copy.getSchema());
        Assert.assertEquals("schema", clusterInfo.getSchema());
    }

    @Test
    public void testClusterInfoImplCopy() {
        ClusterInfo clusterInfo = createClusterInfo(1);
        ClusterInfo copy = clusterInfo.copy();
        assertEquals(clusterInfo, copy);
        Assert.assertNotSame(clusterInfo, copy);
        Assert.assertTrue(clusterInfo instanceof ClusterInfoImpl);

        copy.setSchema("schema-copy");
        Assert.assertEquals("schema-copy", copy.getSchema());
        Assert.assertEquals("schema", clusterInfo.getSchema());
    }

    private ClusterInfo createClusterInfo(int generation) {
        return new ClusterInfoBuilder()
                .name("name")
                .schema("schema")
                .numberOfInstances(2)
                .numberOfBackups(1)
                .instanceId(1)
                .backupId(1)
                .generation(generation)
                .build();
    }

    private void assertEquals(ClusterInfo ci1, ClusterInfo ci2) {
        Assert.assertEquals(ci1.getName(), ci2.getName());
        Assert.assertEquals(ci1.getSchema(), ci2.getSchema());
        Assert.assertEquals(ci1.getNumberOfInstances(), ci2.getNumberOfInstances());
        Assert.assertEquals(ci1.getNumberOfBackups(), ci2.getNumberOfBackups());
        Assert.assertEquals(ci1.getInstanceId(), ci2.getInstanceId());
        Assert.assertEquals(ci1.getBackupId(), ci2.getBackupId());
        Assert.assertEquals(ci1.getGeneration(), ci2.getGeneration());
    }
}
