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
    private static final Logger logger = LoggerFactory.getLogger(SqlParserTest.class);
    private final List<String> inputs = Arrays.asList(
            "SELECT REGION,ID_BB_PARSEKEY,EXCH_CODE,NAME,CRNCY,FLEX_ID,ID_BB_GLOBAL,ID_SEDOL1,ID_SEDOL2,SECURITY_TYP,EQY_SH_OUT,ID_BB_GLOBAL_COMPANY FROM com.gigaspaces.test.database.jdbc.gdriver.poc.generator.model.bloomberg.bloomberg_tzero_out WHERE REGION = ? AND DDATE = ?"
            , /*parsed successfully without changes */ "SELECT TICKER, LAST_PRICE, DBTIMESTAMP\n" +
                    "FROM BPIPE_RT.REAL_TIME t1\n" +
                    "WHERE TICKER LIKE '%Curncy%'\n" +
                    "and DATA_RECEIVED_TIMESTAMP = (SELECT MAX(DATA_RECEIVED_TIMESTAMP) FROM BPIPE_RT.REAL_TIME t2 WHERE t1.TICKER=t2.TICKER and LAST_PRICE is not null)\n" +
                    "ORDER BY DATA_RECEIVED_TIMESTAMP DESC;"
            , /*parsed successfully without changes */ "SELECT COUNTRY,CRNCY,ID_BB_GLOBAL,ID_BB_SEC_NUM_DES,EXCH_CODE,ID_SEDOL1,ID_SEDOL2,NAME,ID_BB_GLOBAL_COMPANY,AMT_OUTSTANDING,SECURITY_TYP  FROM com.gigaspaces.test.database.jdbc.gdriver.poc.generator.model.bloomberg_fi.corp_pfd_out WHERE REGION = ? AND DATA_DATE = ? AND MARKET_SECTOR_DES = 'Pfd' AND COUNTRY IN ('US', 'CA') AND CALLED_BOOL = 'N' AND EXCH_CODE <> 'NOT LISTED'"
            , /*parsed successfully without changes */ "SELECT btz.FLEX_ID, badj.CLOSE_PX_ADJ FROM com.gigaspaces.test.database.jdbc.gdriver.poc.generator.model.bloomberg.bloomberg_tzero_out btz, com.gigaspaces.test.database.jdbc.gdriver.poc.generator.model.bloomberg.bloomberg_tzero_px_out_adjusted badj WHERE btz.REGION = ? and btz.DDATE = ? AND btz.DDATE = badj.OPEN_DATE AND btz.ID_BB_GLOBAL = badj.ID_BB_GLOBAL"

            , /*barak*/ "select * from a, b"
            , /*barak*/ "select * from (select a from b) as a, b"
            ,/*barak*/ "SELECT REPLACE(x.TICKER, ' ELEC ', ' ') as TICKER," +
                    "MAX(DATA_RECEIVED_TIMESTAMP) AS DATA_RECEIVED_TIMESTAMP,\n" +
                    "MAX(CRNCY) AS CRNCY\n" +
                    "FROM BPIPE_RT.REAL_TIME_FUTURES as x\n"
            ,/*barak*/ "SELECT * FROM A GROUP BY X"
            ,/*barak*/ "select * from (select * from a as x INNER JOIN (SELECT * from b) as y ON foo = bar)"
            ,/*barak*/ "select * from a as x INNER JOIN (SELECT * from b) as y ON foo = bar\n" +
                    "where t = f\n" +
                    "GROUP BY foo(a, b)"
            , /*barak*/ "select first_name, last_name, order_date, order_amount\n" +
                    "from customers c\n" +
                    "full join orders o\n" +
                    "on c.customer_id = o.customer_id"

            , "SELECT *\n" +
                    "FROM (SELECT REPLACE(x.TICKER, ' ELEC ', ' ') as TICKER,\n" +
                    "      MAX(CRNCY) AS CRNCY,\n" +
                    "      MAX(DATA_RECEIVED_TIMESTAMP) AS DATA_RECEIVED_TIMESTAMP\n" +
                    "      FROM BPIPE_RT.REAL_TIME_FUTURES as x\n" +
                    "      INNER JOIN (SELECT TICKER, MAX(DATA_RECEIVED_TIMESTAMP) AS MAX_TIMESTAMP\n" +
                    "                         FROM BPIPE_RT.REAL_TIME_FUTURES\n" +
                    "                         WHERE CRNCY IS NOT NULL\n" +
                    "                         GROUP BY TICKER) as y ON x.DATA_RECEIVED_TIMESTAMP = MAX_TIMESTAMP and x.TICKER = y.TICKER\n" +
                    "         GROUP BY x.TICKER) RT\n" +
                    "       LEFT JOIN (SELECT x.FX_RATE, x.CURRENCY_CODE, x.DDATE as FX_DATE\n" +
                    "                  FROM BARRA.BARRA_FX_RATES_WITH_MINOR_CURRENCIES AS x\n" +
                    "                         INNER JOIN (SELECT CURRENCY_CODE, MAX(DDATE) as DDATE\n" +
                    "                                     FROM BARRA.BARRA_FX_RATES_WITH_MINOR_CURRENCIES\n" +
                    "                                     GROUP BY CURRENCY_CODE) y\n" +
                    "                           ON x.CURRENCY_CODE = y.CURRENCY_CODE AND x.DDATE = y.DDATE) FX\n" +
                    "         ON RT.CRNCY = FX.CURRENCY_CODE AND CRNCY IS NOT NULL\n" +
                    "ORDER BY DATA_RECEIVED_TIMESTAMP;\n"

            , "SELECT REPLACE(x.PARSEKYABLE_DES_SOURCE, ' ELEC ', ' ') AS TICKER, MAX(PX_YEST_CLOSE) as PX_YEST_CLOSE, MAX(x.DBTIMESTAMP) as DBTIMESTAMP\n" +
                    "FROM BLOOMBERG_FUTURES.SHARE_FUTURES_PRICING_RPX as x\n" +
                    "       INNER JOIN (SELECT PARSEKYABLE_DES_SOURCE, MAX(DBTIMESTAMP) as MAX_TIMESTAMP\n" +
                    "                   FROM BLOOMBERG_FUTURES.SHARE_FUTURES_PRICING_RPX\n" +
                    "                   WHERE PX_YEST_CLOSE is not null\n" +
                    "                     AND DATA_DATE > CURRENT_DATE - 10\n" +
                    "    GROUP BY PARSEKYABLE_DES_SOURCE) as y\n" +
                    "    ON x.DBTIMESTAMP = MAX_TIMESTAMP and x.PARSEKYABLE_DES_SOURCE = y.PARSEKYABLE_DES_SOURCE\n" +
                    "WHERE PX_YEST_CLOSE is not null\n" +
                    "GROUP BY REPLACE(x.PARSEKYABLE_DES_SOURCE, ' ELEC ', ' ')"


            , "SELECT m.TICKER, m.LAST_PRICE\n" +
                    "FROM (SELECT x.TICKER, MAX(LAST_PRICE) as LAST_PRICE\n" +
                    "      FROM BPIPE_RT.REAL_TIME as x\n" +
                    "             INNER JOIN (SELECT TICKER, MAX(DATA_RECEIVED_TIMESTAMP) as MAX_TIMESTAMP\n" +
                    "                         FROM BPIPE_RT.REAL_TIME\n" +
                    "                         WHERE LAST_PRICE is not null\n" +
                    "                           AND LAST_PRICE > 0\n" +
                    "                           AND DATA_DATE > CURRENT_DATE - 10\n" +
                    "                         GROUP BY TICKER) as y\n" +
                    "               ON x.DATA_RECEIVED_TIMESTAMP = MAX_TIMESTAMP and x.TICKER = y.TICKER\n" +
                    "      GROUP BY x.TICKER) m\n" +
                    "where m.ticker in ('EPRA Index','F3MNG Index','F3RETG Index','GSCBLUXL Index','GSCBMUAE Index','GSCBOSA1 Index','GSCBOSAU Index','GSCBQTAR Index','GSCBSUDN Index','GSCBUKD1 Index','GSEXACOR Index','GSEXADIS Index','GSEXCINT Index','GSEXCRP1 Index','GSEXNPTC Index','GSEXRGAS Index','GSEXRUS1 Index','GSEXSF8X Index','JP55BOV5 Index','JPEPEMMC Index','JPEPJPMC Index','JPEPKRMC Index','JPEPTHM2 Index','JPEPTWM2 Index','JPEXBOV3 Index','JPEXBOV5 Index','JPEXBOV6 Index','JPEXCAPD Index','JPEXGSFT Index','JPEXHDG2 Index','JPEXHDG7 Index','JPEXSAUD Index','JPEXSEM Index','JPEXSFT Index','JPEXTMS2 Index','JPEXTS40 Index','JPEXTS50 Index','LCXP Index','M1SAP Index','MCX Index','MCXP Index','MDAX Index','MGCUAE Index','MGCUQA Index','MSEPAPEJ Index','MSEPCNAM Index','MSEPCRD1 Index','MSEPCRD2 Index','MSEPCRWD Index','MSEPEUNT Index','MSEPGRL1 Index','MSEPHFLY Index','MSEPHIFI Index','MSEPHMOM Index','MSEPIDMA Index','MSEPLMOM Index','MSEPMOMO Index','MSEPNAOS Index','MSEPQLCY Index','MSEPSMOM Index','MSEPSMSW Index','MSEPUGRL Index','MSEPUMOL Index','MSEPUMOS Index','MSEPWTPE Index','MSQQEVLL Index','MXAE Index','MXQA Index','NDEUTHF Index','SCXP Index','SMI Index','SX3E Index','SX3P Index','SX4P Index','SX5E Index','SX6P Index','SX7E Index','SX7P Index','SX86P Index','SX8P Index','SXAP Index','SXDP Index','SXEP Index','SXFP Index','SXIP Index','SXKP Index','SXMP Index','SXNP Index','SXOP Index','SXPP Index','SXQP Index','SXRP Index','SXTP Index','SXXP Index','TSEREIT Index','UKX Index');\n" +
                    "\n" +
                    "\n"

    );

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
