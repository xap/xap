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

package com.j_spaces.jdbc;

import com.gigaspaces.client.transaction.ITransactionManagerProvider.TransactionManagerType;
import com.gigaspaces.client.transaction.LookupTransactionManagerConfiguration;
import com.gigaspaces.client.transaction.TransactionManagerConfiguration;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.Constants.QueryProcessorInfo;
import com.j_spaces.core.JSpaceAttributes;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Configuration object for the QueryProcessor
 *
 * @author anna
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class QueryProcessorConfiguration {
    //logger 
    final private static Logger _logger = Logger.getLogger(Constants.LOGGER_QUERY);

    private static final String TRACE_EXEC_TIME_PROPERTY = "TRACE_EXEC_TIME";

    private static final String AUTO_COMMIT_PROPERTY = "AUTO_COMMIT";

    private static final String PARSER_CASE_SENSETIVITY_PROPERTY = "PARSER_CASE_SENSETIVITY";

    private static final String TRANSACTION_TIMEOUT_PROPERTY = "TRANSACTION_TIMEOUT";

    private static final String SPACE_WRITE_LEASE_PROPERTY = "SPACE_WRITE_LEASE";

    private static final String SPACE_READ_LEASE_TIME_PROPERTY = "SPACE_READ_LEASE_TIME";

    private static final String SPACE_URL = "SPACE_URL";

    private static final String PORT_PROPERTY = "PORT";

    private static final int PORT_DEFAULT = 2872;

    private static final String DATE_FORMAT_PROPERTY = "DATE_FORMAT";
    private static final String DATE_TIME_FORMAT_PROPERTY = "DATETIME_FORMAT";
    private static final String TIME_FORMAT_PROPERTY = "TIME_FORMAT";

    private int _readLease = Integer.parseInt(QueryProcessorInfo.QP_SPACE_READ_LEASE_TIME_DEFAULT);
    private long _writeLease = Long.parseLong(QueryProcessorInfo.QP_SPACE_WRITE_LEASE_DEFAULT);
    private long _transactionTimeout = Integer.parseInt(QueryProcessorInfo.QP_TRANSACTION_TIMEOUT_DEFAULT);
    private boolean _parserCaseSensitivity = Boolean.parseBoolean(QueryProcessorInfo.QP_PARSER_CASE_SENSETIVITY_DEFAULT);
    private String _dateFormat = QueryProcessorInfo.QP_DATE_FORMAT_DEFAULT;
    private String _dateTimeFormat = QueryProcessorInfo.QP_DATETIME_FORMAT_DEFAULT;
    private String _timeFormat = QueryProcessorInfo.QP_TIME_FORMAT_DEFAULT;
    private boolean _traceExecTime = Boolean.parseBoolean(QueryProcessorInfo.QP_TRACE_EXEC_TIME_DEFAULT);
    private boolean _autoCommit = Boolean.parseBoolean(QueryProcessorInfo.QP_AUTO_COMMIT_DEFAULT);
    private String _spaceURL;

    private int _listenPort = PORT_DEFAULT;

    private TransactionManagerConfiguration _transactionManagerConfiguration;

    public QueryProcessorConfiguration(JSpaceAttributes conf, Properties localProps) {
        // set properties from space
        if (conf != null) {
            _readLease = conf.getQpSpaceReadLeaseTime();
            _writeLease = conf.getQpSpaceWriteLeaseTime();
            _transactionTimeout = conf.getQpTransactionTimeout();
            _parserCaseSensitivity = conf.isQPParserCaseSensetivity();
            _autoCommit = conf.isQPAutoCommit();
            _traceExecTime = conf.isQPTraceExecTime();
            _dateFormat = conf.getQpDateFormat();
            _dateTimeFormat = conf.getQpDateTimeFormat();
            _timeFormat = conf.getQpTimeFormat();
        }

        // set properties from override properties file
        configure(localProps);

        if (_logger.isLoggable(Level.CONFIG)) {
            _logger.config("\n QueryProcessor configuration:\n\t"
                    + "parserCaseSensitivity=" + _parserCaseSensitivity + "\n\t"
                    + "writeLease=" + _writeLease + "\n" + "\t"
                    + "readLease=" + _readLease + "\n" + "\t"
                    + "transactionTimeout=" + _transactionTimeout + "\n\t"
                    + "autoCommit=" + _autoCommit + "\n\t"
                    + "traceExecTime=" + _traceExecTime + "\n\t"
                    + "dateFormat=" + _dateFormat + "\n\t"
                    + "dateTimeFormat=" + _dateTimeFormat + "\n\t"
                    + "timeFormat=" + _timeFormat
            );
        }
    }

    private void configure(Properties localProps) {
        if (localProps == null)
            return;

        _readLease = getInteger(localProps.getProperty(SPACE_READ_LEASE_TIME_PROPERTY), _readLease);
        _writeLease = getLong(localProps.getProperty(SPACE_WRITE_LEASE_PROPERTY), _writeLease);
        _transactionTimeout = getLong(localProps.getProperty(TRANSACTION_TIMEOUT_PROPERTY), _transactionTimeout);
        _parserCaseSensitivity = getBoolean(localProps.getProperty(PARSER_CASE_SENSETIVITY_PROPERTY), _parserCaseSensitivity);
        _autoCommit = getBoolean(localProps.getProperty(AUTO_COMMIT_PROPERTY), _autoCommit);
        _traceExecTime = getBoolean(localProps.getProperty(TRACE_EXEC_TIME_PROPERTY), _traceExecTime);
        _dateFormat = localProps.getProperty(DATE_FORMAT_PROPERTY, _dateFormat);
        _dateTimeFormat = localProps.getProperty(DATE_TIME_FORMAT_PROPERTY, _dateTimeFormat);
        _timeFormat = localProps.getProperty(TIME_FORMAT_PROPERTY, _timeFormat);
        _spaceURL = localProps.getProperty(SPACE_URL);
        _listenPort = getInteger(localProps.getProperty(PORT_PROPERTY), PORT_DEFAULT);

        // Get JDBC transaction configuration                      
        String txnType = localProps.getProperty(QueryProcessorInfo.QP_TRANSACTION_TYPE, QueryProcessorInfo.QP_TRANSACTION_TYPE_DEFAULT);
        TransactionManagerType transactionManagerType = TransactionManagerType.getValue(txnType);

        if (transactionManagerType == null)
            transactionManagerType = TransactionManagerType.DISTRIBUTED;

        _transactionManagerConfiguration = TransactionManagerConfiguration.newConfiguration(transactionManagerType);

        if (transactionManagerType == TransactionManagerType.LOOKUP_DISTRIBUTED) {
            LookupTransactionManagerConfiguration lookupConfiguration = (LookupTransactionManagerConfiguration) _transactionManagerConfiguration;
            lookupConfiguration.setLookupTransactionName(localProps.getProperty(QueryProcessorInfo.QP_LOOKUP_TRANSACTION_NAME,
                    QueryProcessorInfo.QP_LOOKUP_TRANSACTION_NAME_DEFAULT));
            lookupConfiguration.setLookupTransactionGroups(localProps.getProperty(QueryProcessorInfo.QP_LOOKUP_TRANSACTION_GROUPS,
                    SystemInfo.singleton().lookup().defaultGroups()));
            lookupConfiguration.setLookupTransactionLocators(localProps.getProperty(QueryProcessorInfo.QP_LOOKUP_TRANSACTION_LOCATORS,
                    QueryProcessorInfo.QP_LOOKUP_TRANSACTION_LOCATORS_DEFAULT));
            lookupConfiguration.setLookupTransactionTimeout(getLong(localProps.getProperty(QueryProcessorInfo.QP_LOOKUP_TRANSACTION_TIMEOUT),
                    QueryProcessorInfo.QP_LOOKUP_TRANSACTION_TIMEOUT_DEFAULT));
        }
    }

    private static boolean getBoolean(String val, boolean defaultVal) {
        return val != null ? Boolean.parseBoolean(val) : defaultVal;
    }

    private static int getInteger(String val, int defaultVal) {
        return val != null ? Integer.parseInt(val) : defaultVal;
    }

    private static long getLong(String val, long defaultVal) {
        return val != null ? Long.parseLong(val) : defaultVal;
    }

    public int getReadLease() {
        return _readLease;
    }

    public void setReadLease(int readLease) {
        _readLease = readLease;
    }

    public long getWriteLease() {
        return _writeLease;
    }

    public void setWriteLease(long writeLease) {
        _writeLease = writeLease;
    }

    public long getTransactionTimeout() {
        return _transactionTimeout;
    }

    public void setTransactionTimeout(long transactionTimeout) {
        _transactionTimeout = transactionTimeout;
    }

    public boolean isParserCaseSensitivity() {
        return _parserCaseSensitivity;
    }

    public void setParserCaseSensitivity(boolean parserCaseSensitivity) {
        _parserCaseSensitivity = parserCaseSensitivity;
    }

    public String getDateFormat() {
        return _dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        _dateFormat = dateFormat;
    }

    public String getDateTimeFormat() {
        return _dateTimeFormat;
    }

    public void setDateTimeFormat(String dateTimeFormat) {
        _dateTimeFormat = dateTimeFormat;
    }

    public String getTimeFormat() {
        return _timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        _timeFormat = timeFormat;
    }

    public boolean isTraceExecTime() {
        return _traceExecTime;
    }

    public void setTraceExecTime(boolean traceExecTime) {
        _traceExecTime = traceExecTime;
    }

    public boolean isAutoCommit() {
        return _autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        _autoCommit = autoCommit;
    }

    public String getSpaceURL() {
        return _spaceURL;
    }

    public void setSpaceURL(String spaceURL) {
        _spaceURL = spaceURL;
    }

    public int getListenPort() {
        return _listenPort;
    }

    public void setListenPort(int listenPort) {
        _listenPort = listenPort;
    }

    public TransactionManagerConfiguration getTransactionManagerConfiguration() {
        return _transactionManagerConfiguration;
    }

}
