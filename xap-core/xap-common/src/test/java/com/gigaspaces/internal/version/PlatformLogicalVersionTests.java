package com.gigaspaces.internal.version;

import org.junit.Assert;
import org.junit.Test;

public class PlatformLogicalVersionTests {
    @Test
    public void comparisonTest() {
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
}
