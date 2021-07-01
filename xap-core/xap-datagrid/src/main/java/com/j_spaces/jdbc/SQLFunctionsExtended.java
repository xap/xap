package com.j_spaces.jdbc;

import com.gigaspaces.query.sql.functions.*;
import com.gigaspaces.query.sql.functions.extended.ChrSqlFunction;
import com.gigaspaces.query.sql.functions.extended.CoalesceSqlFunction;

import java.util.HashMap;
import java.util.Map;

public class SQLFunctionsExtended {
    private static final Map<String, SqlFunction> functions = new HashMap<>();

    static {
        functions.put("CEILING", new CeilSqlFunction());
        functions.put("CHR", new ChrSqlFunction());
        functions.put("CHAR", new ChrSqlFunction());
        functions.put("COALESCE", new CoalesceSqlFunction());
        functions.put("LCASE", new LowerSqlFunction());
        functions.put("UCASE", new UpperSqlFunction());
        functions.put("LEFT", new LeftSqlFunction());
        functions.put("RIGHT", new RightSqlFunction());
        functions.put("CAST", new CastSqlFunction());
        functions.put("REPEAT", new RepeatSqlFunction());
//        functions.put("SUBSTR", new SubstrSqlFunction());

        functions.put("CURRENT_DATE", new CurrentDateSqlFunction());
        functions.put("CURRENT_TIME", new CurrentTimeSqlFunction());
        functions.put("TRUNCATE", new TruncSqlFunction());
    }


    public static Map<String, SqlFunction> getFunctions() {
        return functions;
    }
}
