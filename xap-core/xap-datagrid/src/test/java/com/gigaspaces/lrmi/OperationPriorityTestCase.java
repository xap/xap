package com.gigaspaces.lrmi;

import com.gigaspaces.lrmi.nio.RequestPacket;
import org.junit.Assert;
import org.junit.Test;

public class OperationPriorityTestCase {
    final byte NONE = 0;
    // These values are intentionally duplicated from RequestPacket.BitMap for backwards testability.
    final byte REGULAR = 0;
    final byte LIVENESS = 1 << 3;
    final byte MONITORING = 1 << 4;
    final byte CUSTOM = 1 << 5;
    // Mock values to test bitness smaller and greater than priority mock values.
    final byte MOCK_BEFORE = 1 << 2;
    final byte MOCK_AFTER = 1 << 6;

    @Test
    public void testPriorityBackwards() {
        Assert.assertSame(REGULAR, RequestPacket.encodePriority(OperationPriority.REGULAR, NONE));
        Assert.assertSame(LIVENESS, RequestPacket.encodePriority(OperationPriority.LIVENESS, NONE));
        Assert.assertSame(MONITORING, RequestPacket.encodePriority(OperationPriority.MONITORING, NONE));
        Assert.assertSame(CUSTOM, RequestPacket.encodePriority(OperationPriority.CUSTOM, NONE));
    }

    @Test
    public void testPriorityEncodeDecode() {
        for (OperationPriority priority : OperationPriority.values()) {
            test(priority, NONE);
            test(priority, MOCK_BEFORE);
            test(priority, MOCK_AFTER);
            test(priority, (byte) (MOCK_BEFORE | MOCK_AFTER));
        }
    }

    private void test(OperationPriority priority, byte initialFlags) {
        byte actualFlags = RequestPacket.encodePriority(priority, initialFlags);
        OperationPriority actual = RequestPacket.decodePriority(actualFlags);
        Assert.assertEquals(priority, actual);
        byte pureFlags = RequestPacket.encodePriority(priority, NONE);
        Assert.assertEquals(actualFlags - pureFlags, initialFlags);
    }
}
