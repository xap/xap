package com.j_spaces.jdbc.parser.grammar;

import com.j_spaces.jdbc.Query;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SqlParserTest {
    private static Logger logger = LoggerFactory.getLogger(SqlParserTest.class);
    private final List<String> inputs = Arrays.asList("SELECT REGION,ID_BB_PARSEKEY,EXCH_CODE,NAME,CRNCY,FLEX_ID,ID_BB_GLOBAL,ID_SEDOL1,ID_SEDOL2,SECURITY_TYP,EQY_SH_OUT,ID_BB_GLOBAL_COMPANY FROM bloomberg.bloomberg_tzero_out WHERE REGION = ? AND DDATE = ?");

    public SqlParserTest() {
        logger.info("creating SqlParserTest");
    }

    @TestFactory
    public List<DynamicTest> testSelects() {
        return inputs.stream().map(i -> DynamicTest.dynamicTest("parsing: " + i, () -> parse(i))).collect(Collectors.toList());
    }


    @SuppressWarnings("UnusedReturnValue")
    private Query parse(String input) throws ParseException {
        SqlParser parser = new SqlParser(new ByteArrayInputStream(input.getBytes()));
        return parser.parseStatement();
    }

}
