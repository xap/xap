package com.gigaspaces.jdbc.calcite.schema.type;

public class TypeBytea extends PgType {
    public static final PgType INSTANCE = new TypeBytea();

    private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

    private static byte getHex(byte b) {
        // 0-9 == 48-57
        if (b <= 57) {
            return (byte) (b - 48);
        }

        // a-f == 97-102
        if (b >= 97) {
            return (byte) (b - 97 + 10);
        }

        // A-F == 65-70
        return (byte) (b - 65 + 10);
    }

    public TypeBytea() {
        super(17, "bytea", -1, 1001, 0);
    }

}
