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

package com.gigaspaces.internal.utils.parsers;

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.UUID;

public class ConventionObjectParserTests {
    @Test
    public void testConventionParsers() throws SQLException {
        // parse(String)
        testParser(new Type1(1));
        // parse(CharSequence)
        testParser(new Type2(1));
        // parse(Object)
        testParser(new Type3(1));
        // valueOf(String)
        testParser(new Type4(1));
        // valueOf(CharSequence)
        testParser(new Type5(1));
        // valueOf(Object)
        testParser(new Type6(1));
        // ctor(String)
        testParser(new Type7(1));
        // ctor(CharSequence)
        testParser(new Type8(1));
        // ctor(Object)
        testParser(new Type9(1));

        testParserFromString(new TypeWithUuid());
    }

    private void testParser(Object obj) throws SQLException {
        final AbstractParser parser = ConventionObjectParser.getConventionParserIfAvailable(obj.getClass());
        Assert.assertNotNull(parser);
        Assert.assertEquals(obj, parser.parse("1"));
    }

    private void testParserFromString(TypeWithUuid obj) throws SQLException {
        final AbstractParser parser = ConventionObjectParser.getConventionParserIfAvailable(obj.value.getClass());
        Assert.assertNotNull(parser);
        Assert.assertEquals(obj.value,(parser.parse(obj.value.toString())));
    }

    public static class Type1 {
        private final int value;

        public Type1(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            return ((Type1) obj).value == value;
        }

        public static Type1 parse(String s) {
            return new Type1(Integer.parseInt(s));
        }
    }

    public static class Type2 {
        private final int value;

        public Type2(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            return ((Type2) obj).value == value;
        }

        public static Type2 parse(CharSequence s) {
            return new Type2(Integer.parseInt(s.toString()));
        }
    }

    public static class Type3 {
        private final int value;

        public Type3(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            return ((Type3) obj).value == value;
        }

        public static Type3 parse(Object s) {
            return new Type3(Integer.parseInt(s.toString()));
        }
    }

    public static class Type4 {
        private final int value;

        public Type4(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            return ((Type4) obj).value == value;
        }

        public static Type4 valueOf(String s) {
            return new Type4(Integer.parseInt(s));
        }
    }

    public static class Type5 {
        private final int value;

        public Type5(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            return ((Type5) obj).value == value;
        }

        public static Type5 valueOf(CharSequence s) {
            return new Type5(Integer.parseInt(s.toString()));
        }
    }

    public static class Type6 {
        private final int value;

        public Type6(int value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object obj) {
            return ((Type6) obj).value == value;
        }

        public static Type6 valueOf(Object s) {
            return new Type6(Integer.parseInt(s.toString()));
        }
    }

    public static class Type7 {
        private final int value;

        public Type7(int value) {
            this.value = value;
        }

        public Type7(String value) {
            this.value = Integer.parseInt(value);
        }

        @Override
        public boolean equals(Object obj) {
            return ((Type7) obj).value == value;
        }
    }

    public static class Type8 {
        private final int value;

        public Type8(int value) {
            this.value = value;
        }

        public Type8(CharSequence value) {
            this.value = Integer.parseInt(value.toString());
        }

        @Override
        public boolean equals(Object obj) {
            return ((Type8) obj).value == value;
        }
    }

    public static class Type9 {
        private final int value;

        public Type9(int value) {
            this.value = value;
        }

        public Type9(Object value) {
            this.value = Integer.parseInt(value.toString());
        }

        @Override
        public boolean equals(Object obj) {
            return ((Type9) obj).value == value;
        }
    }

    public static class TypeWithUuid {
        private final UUID value;

        public TypeWithUuid() {
            UUID uuid = UUID.randomUUID();
            this.value = uuid;
        }
    }
}
