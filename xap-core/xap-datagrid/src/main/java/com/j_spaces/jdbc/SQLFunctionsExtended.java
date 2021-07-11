package com.j_spaces.jdbc;

import com.gigaspaces.query.sql.functions.*;
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
        functions.put("LEFT", new LeftSqlFunction());
        functions.put("RIGHT", new RightSqlFunction());
        functions.put("CAST", new CastSqlFunction());
        functions.put("REPEAT", new RepeatSqlFunction());
        functions.put("CURRENT_DATE", new CurrentDateSqlFunction());
        functions.put("CURRENT_TIME", new CurrentTimeSqlFunction());
        functions.put("ODBC_TIMESTAMP", new OdbcTimestampSqlFunction());
        functions.put("DAYNAME", new DayNameSqlFunction());
        functions.put("DAYOFMONTH", new DayOfMonthSqlFunction());
        functions.put("DAYOFWEEK", new DayOfWeekSqlFunction());
        functions.put("DAYOFYEAR", new DayOfYearSqlFunction());
        functions.put("HOUR", new HourSqlFunction());
        functions.put("MINUTE", new MinuteSqlFunction());
        functions.put("MONTH", new MonthSqlFunction());
        functions.put("MONTHNAME", new MonthNameSqlFunction());
        functions.put("QUARTER", new QuarterSqlFunction());
        functions.put("SECOND", new SecondSqlFunction());
        functions.put("WEEK", new WeekSqlFunction());
        functions.put("YEAR", new YearSqlFunction());

        functions.put("TRUNCATE", new TruncSqlFunction());
        functions.put("TRUNC", new TruncSqlFunction());
        functions.put("LOG", new LogSqlFunction());
        functions.put("LOG10", new Log10SqlFunction());
        functions.put("LN", new LnSqlFunction());
        functions.put("POWER", new PowerSqlFunction());
        functions.put("RANDOM", new RandomSqlFunction());
        functions.put("STRPOS", new StrposFunction());
        functions.put("SUBSTR", new SubstrSqlFunction());
        functions.put("SUBSTRING", new SubstringSqlFunction());

        functions.put("ARRAY_LOWER", new UnsupportedSqlFunction("ARRAY_LOWER"));
        functions.put("ARRAY_UPPER", new UnsupportedSqlFunction("ARRAY_UPPER"));
        functions.put("GENERATE_SERIES", new UnsupportedSqlFunction("GENERATE_SERIES"));
        functions.put("PG_GET_EXPR", new PgGetExprSqlFunction());
    }

    public static Map<String, SqlFunction> getFunctions() {
        return functions;
    }
}
