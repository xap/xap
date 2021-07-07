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
