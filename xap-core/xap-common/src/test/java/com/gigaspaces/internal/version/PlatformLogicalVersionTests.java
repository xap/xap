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
package com.gigaspaces.internal.version;

import org.junit.Assert;
import org.junit.Test;

public class PlatformLogicalVersionTests {
    @Test
    public void fromBuildTest() {
        byte major = 0;
        byte minor = 0;
        byte sp = 0;
        PlatformLogicalVersion version1 = PlatformLogicalVersion.fromBuild(major, minor, sp, 100);
        PlatformLogicalVersion version1_1 = PlatformLogicalVersion.fromBuild(major, minor, sp, 100, 10);
        PlatformLogicalVersion version2 = PlatformLogicalVersion.fromBuild(major, minor, sp, 200);

        Assert.assertEquals(0, version1.compareTo(version1));
        Assert.assertFalse(version1.lessThan(version1));
        Assert.assertTrue(version1.greaterOrEquals(version1));

        Assert.assertEquals(-1, version1.compareTo(version2));
        Assert.assertTrue(version1.lessThan(version2));
        Assert.assertFalse(version1.greaterOrEquals(version2));

        Assert.assertEquals(1, version2.compareTo(version1));
        Assert.assertFalse(version2.lessThan(version1));
        Assert.assertTrue(version2.greaterOrEquals(version1));

        Assert.assertEquals(-1, version1.compareTo(version1_1));
        Assert.assertTrue(version1.lessThan(version1_1));
        Assert.assertFalse(version1.greaterOrEquals(version1_1));

        Assert.assertEquals(1, version1_1.compareTo(version1));
        Assert.assertFalse(version1_1.lessThan(version1));
        Assert.assertTrue(version1_1.greaterOrEquals(version1));
    }

    @Test
    public void fromVersionTest() {
        PlatformLogicalVersion version1 = PlatformLogicalVersion.fromVersion(14, 0, 0);
        PlatformLogicalVersion version1_1 = PlatformLogicalVersion.fromVersion(14, 1, 0);
        PlatformLogicalVersion version1_1_1 = PlatformLogicalVersion.fromVersion(14, 1, 1);
        PlatformLogicalVersion version2 = PlatformLogicalVersion.fromVersion(15, 0, 0);

        Assert.assertEquals(0, version1.compareTo(version1));
        Assert.assertFalse(version1.lessThan(version1));
        Assert.assertTrue(version1.greaterOrEquals(version1));

        Assert.assertEquals(-1, version1.compareTo(version2));
        Assert.assertTrue(version1.lessThan(version2));
        Assert.assertFalse(version1.greaterOrEquals(version2));

        Assert.assertEquals(1, version2.compareTo(version1));
        Assert.assertFalse(version2.lessThan(version1));
        Assert.assertTrue(version2.greaterOrEquals(version1));

        Assert.assertEquals(-1, version1.compareTo(version1_1));
        Assert.assertTrue(version1.lessThan(version1_1));
        Assert.assertFalse(version1.greaterOrEquals(version1_1));

        Assert.assertEquals(1, version1_1.compareTo(version1));
        Assert.assertFalse(version1_1.lessThan(version1));
        Assert.assertTrue(version1_1.greaterOrEquals(version1));

        Assert.assertEquals(-1, version1.compareTo(version1_1_1));
        Assert.assertTrue(version1.lessThan(version1_1_1));
        Assert.assertFalse(version1.greaterOrEquals(version1_1_1));

        Assert.assertEquals(1, version1_1_1.compareTo(version1));
        Assert.assertFalse(version1_1_1.lessThan(version1));
        Assert.assertTrue(version1_1_1.greaterOrEquals(version1));


        Assert.assertEquals(-1, version1_1.compareTo(version1_1_1));
        Assert.assertTrue(version1_1.lessThan(version1_1_1));
        Assert.assertFalse(version1_1.greaterOrEquals(version1_1_1));

        Assert.assertEquals(1, version1_1_1.compareTo(version1_1));
        Assert.assertFalse(version1_1_1.lessThan(version1_1));
        Assert.assertTrue(version1_1_1.greaterOrEquals(version1_1));
    }

    @Test
    public void buildVersionBackwardsTest() {
        PlatformLogicalVersion vOld = PlatformLogicalVersion.v12_0_0;
        PlatformLogicalVersion vNew= PlatformLogicalVersion.v14_5_0;

        Assert.assertEquals(-1, vOld.compareTo(vNew));
        Assert.assertTrue(vOld.lessThan(vNew));
        Assert.assertFalse(vOld.greaterOrEquals(vNew));

        Assert.assertEquals(1, vNew.compareTo(vOld));
        Assert.assertFalse(vNew.lessThan(vOld));
        Assert.assertTrue(vNew.greaterOrEquals(vOld));
    }

    @Test
    public void patchTest() {
        PlatformLogicalVersion vOld = PlatformLogicalVersion.v12_3_0_PATCH4;
        PlatformLogicalVersion a1= PlatformLogicalVersion.fromVersion(14, 5, 0, "a", 1);
        PlatformLogicalVersion a2= PlatformLogicalVersion.fromVersion(14, 5, 0, "a", 2);
        PlatformLogicalVersion b1= PlatformLogicalVersion.fromVersion(14, 5, 0, "b", 1);

        Assert.assertTrue(a2.patchSameOrGreater(a2));
        Assert.assertTrue(a2.patchSameOrGreater(a1));
        Assert.assertFalse(a2.patchSameOrGreater(b1));
        Assert.assertFalse(a2.patchSameOrGreater(vOld));

        Assert.assertTrue(b1.patchSameOrGreater(b1));
        Assert.assertFalse(b1.patchSameOrGreater(a1));
        Assert.assertFalse(b1.patchSameOrGreater(a2));
        Assert.assertFalse(b1.patchSameOrGreater(vOld));

        Assert.assertTrue(a1.patchSameOrGreater(a1));
        Assert.assertFalse(a1.patchSameOrGreater(a2));
        Assert.assertFalse(a1.patchSameOrGreater(b1));
        Assert.assertFalse(a1.patchSameOrGreater(vOld));

        Assert.assertTrue(vOld.patchSameOrGreater(vOld));
        Assert.assertFalse(vOld.patchSameOrGreater(a1));
        Assert.assertFalse(vOld.patchSameOrGreater(a2));
        Assert.assertFalse(vOld.patchSameOrGreater(b1));

    }

}
