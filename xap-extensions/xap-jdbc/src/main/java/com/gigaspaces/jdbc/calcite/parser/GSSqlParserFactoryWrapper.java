package com.gigaspaces.jdbc.calcite.parser;

import com.gigaspaces.jdbc.calcite.parser.generated.GSSqlParserImpl;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;

import java.io.Reader;

public class GSSqlParserFactoryWrapper implements SqlParserImplFactory {
    public final static SqlParserImplFactory FACTORY = GSSqlParserImpl.FACTORY;
    public final static String FACTORY_CLASS = GSSqlParserFactoryWrapper.class.getName();
    @Override
    public SqlAbstractParserImpl getParser(Reader stream) {
        return GSSqlParserImpl.FACTORY.getParser(stream);
    }

    @Override
    public DdlExecutor getDdlExecutor() {
        return GSSqlParserImpl.FACTORY.getDdlExecutor();
    }
}
