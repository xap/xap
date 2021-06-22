package com.j_spaces.jdbc;

import com.gigaspaces.query.sql.functions.CeilSqlFunction;
import com.gigaspaces.query.sql.functions.LowerSqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.UpperSqlFunction;
import com.gigaspaces.query.sql.functions.extended.*;

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

    }


    public static Map<String, SqlFunction> getFunctions() {
        return functions;
    }
}
