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
package com.gigaspaces.internal.cluster.node.impl;

import org.junit.Assert;
import org.junit.Test;

public class ReplicationUtilsTest {

    @Test
    public void toShortLookupName() {
        String ipV4Lookup = "mySpace_container1_1:mySpace_LocalView_192.168.34.36[13079]_9dfbb274-15ed-44c2-acde-e1445262c475";
        ReplicationUtils utils = new ReplicationUtils();
        Assert.assertEquals("'toShortLookupName' returned wrong result", "LocalView_9dfbb274", utils.toShortLookupName(ipV4Lookup));

        String ipV6Lookup = "mySpace_container1_1:mySpace_LocalView_[fe80::2fdf:844e:7b3d:b57e][13079]_9dfbb274-15ed-44c2-acde-e1445262c475";
        Assert.assertEquals("'toShortLookupName' returned wrong result", "LocalView_9dfbb274", utils.toShortLookupName(ipV6Lookup));

        String ipV4DurLookup = "replication:NotifyDur:192.168.34.36[22321]_c5fe6896-780f-4df8-a976-8f58d68eddbb";
        Assert.assertEquals("'toShortLookupName' returned wrong result", "NotifyDur_c5fe6896", utils.toShortLookupName(ipV4DurLookup));

        String ipV6DurLookup = "replication:NotifyDur:[fe80::2fdf:844e:7b3d:b57e][22321]_c5fe6896-780f-4df8-a976-8f58d68eddbb";
        Assert.assertEquals("'toShortLookupName' returned wrong result", "NotifyDur_c5fe6896", utils.toShortLookupName(ipV6DurLookup));
    }
}