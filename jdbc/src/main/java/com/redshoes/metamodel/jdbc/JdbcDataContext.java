/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.redshoes.metamodel.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import javax.sql.DataSource;

import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.data.EmptyDataSet;
import com.redshoes.metamodel.data.MaxRowsDataSet;
import com.redshoes.metamodel.data.ScalarFunctionDataSet;
import com.redshoes.metamodel.jdbc.dialects.Hive2QueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.HsqldbQueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.IQueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.MysqlQueryRewriter;
import com.redshoes.metamodel.query.AggregateFunction;
import com.redshoes.metamodel.query.CompiledQuery;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.util.FileHelper;
import com.redshoes.metamodel.AbstractDataContext;
import com.redshoes.metamodel.BatchUpdateScript;
import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.MetaModelHelper;
import com.redshoes.metamodel.UpdateScript;
import com.redshoes.metamodel.UpdateSummary;
import com.redshoes.metamodel.UpdateableDataContext;
import com.redshoes.metamodel.jdbc.JdbcUtils.JdbcActionType;
import com.redshoes.metamodel.jdbc.dialects.DB2QueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.DefaultQueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.H2QueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.HiveQueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.ImpalaQueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.OracleQueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.PostgresqlQueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.SQLServerQueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.SQLiteQueryRewriter;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.ColumnTypeImpl;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.SuperColumnType;
import com.redshoes.metamodel.schema.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataContextStrategy to use for JDBC-compliant databases
 */
public class JdbcDataContext extends AbstractDataContext implements UpdateableDataContext {

    public static final String SYSTEM_PROPERTY_BATCH_UPDATES = "metamodel.jdbc.batch.updates";
    public static final String SYSTEM_PROPERTY_CONVERT_LOBS = "metamodel.jdbc.convert.lobs";

    public static final String SYSTEM_PROPERTY_COMPILED_QUERY_POOL_MAX_SIZE =
            "metamodel.jdbc.compiledquery.pool.max.size";
    public static final String SYSTEM_PROPERTY_COMPILED_QUERY_POOL_MIN_EVICTABLE_IDLE_TIME_MILLIS =
            "metamodel.jdbc.compiledquery.pool.idle.timeout";
    public static final String SYSTEM_PROPERTY_COMPILED_QUERY_POOL_TIME_BETWEEN_EVICTION_RUNS_MILLIS =
            "metamodel.jdbc.compiledquery.pool.eviction.period.millis";

    public static final String DATABASE_PRODUCT_POSTGRESQL = DatabaseType.DATABASE_PRODUCT_POSTGRESQL;
    public static final String DATABASE_PRODUCT_MYSQL = DatabaseType.DATABASE_PRODUCT_MYSQL;
    public static final String DATABASE_PRODUCT_HSQLDB = DatabaseType.DATABASE_PRODUCT_HSQLDB;
    public static final String DATABASE_PRODUCT_H2 = DatabaseType.DATABASE_PRODUCT_H2;
    public static final String DATABASE_PRODUCT_SQLSERVER = DatabaseType.DATABASE_PRODUCT_SQLSERVER;
    public static final String DATABASE_PRODUCT_DB2 = DatabaseType.DATABASE_PRODUCT_DB2;
    public static final String DATABASE_PRODUCT_DB2_PREFIX = DatabaseType.DATABASE_PRODUCT_DB2_PREFIX;
    public static final String DATABASE_PRODUCT_ORACLE = DatabaseType.DATABASE_PRODUCT_ORACLE;
    public static final String DATABASE_PRODUCT_HIVE = DatabaseType.DATABASE_PRODUCT_HIVE;
    public static final String DATABASE_PRODUCT_SQLITE = DatabaseType.DATABASE_PRODUCT_SQLITE;
    public static final String DATABASE_PRODUCT_IMPALA = DatabaseType.DATABASE_PRODUCT_IMPALA;
		
    private static final String DEFAULT_SCHEMA_NAME_SQLSERVER = "dbo";

    private static final String SCHEMA_NAME_IDENTIFIER = "TABLE_SCHEM";

    public static final ColumnType COLUMN_TYPE_CLOB_AS_STRING =
            new ColumnTypeImpl("CLOB", SuperColumnType.LITERAL_TYPE, String.class, true);
    public static final ColumnType COLUMN_TYPE_BLOB_AS_BYTES =
            new ColumnTypeImpl("BLOB", SuperColumnType.BINARY_TYPE, byte[].class, true);

    private static final Logger logger = LoggerFactory.getLogger(JdbcDataContext.class);

    private final FetchSizeCalculator _fetchSizeCalculator;
    private final Connection _connection;
    private String _defaultSchemaName;
    private final DataSource _dataSource;
    private final TableType[] _tableTypes;
    private final String _catalogName;
    private final boolean _singleConnection;

    private final MetadataLoader _metadataLoader;

    /**
     * Defines the way that queries are written once dispatched to the database
     */
    private IQueryRewriter _queryRewriter;
    private final String _databaseProductName;
    private final String _databaseVersion;

    /**
     * There are some confusion as to the definition of catalogs and schemas. Some databases separate "groups of tables"
     * by using schemas, others by catalogs. This variable indicates whether a MetaModel schema really represents a
     * catalog.
     */
    private final String _identifierQuoteString;
    private final boolean _supportsBatchUpdates;
    private final boolean _isDefaultAutoCommit;
    private final boolean _usesCatalogsAsSchemas;

    /**
     * Creates the strategy based on a data source, some table types and an optional catalogName
     * 
     * @param dataSource the datasource object to use for making connections
     * @param tableTypes the types of tables to include
     * @param catalogName a catalog name to use, can be null
     */
    public JdbcDataContext(DataSource dataSource, TableType[] tableTypes, String catalogName) {
        this(dataSource, null, tableTypes, catalogName);
    }

    /**
     * Creates the strategy based on a {@link Connection}, some table types and an optional catalogName
     * 
     * @param connection the database connection
     * @param tableTypes the types of tables to include
     * @param catalogName a catalog name to use, can be null
     */
    public JdbcDataContext(Connection connection, TableType[] tableTypes, String catalogName) {
        this(null, connection, tableTypes, catalogName);
    }

    /**
     * Creates the strategy based on a {@link DataSource}, some table types and an optional catalogName
     * 
     * @param dataSource the data source
     * @param tableTypes the types of tables to include
     * @param catalogName a catalog name to use, can be null
     */
    private JdbcDataContext(DataSource dataSource, Connection connection, TableType[] tableTypes, String catalogName) {
        _dataSource = dataSource;
        _connection = connection;
        _tableTypes = tableTypes;
        _catalogName = catalogName;

        if (_dataSource == null) {
            _singleConnection = true;
        } else {
            _singleConnection = false;
        }

        // available memory for fetching is so far fixed at 16 megs.
        _fetchSizeCalculator = new FetchSizeCalculator(16 * 1024 * 1024);

        boolean supportsBatchUpdates = false;
        String identifierQuoteString = null;
        String databaseProductName = null;
        String databaseVersion = null;
        boolean usesCatalogsAsSchemas = false;

        final Connection con = getConnection();

        try {
            _isDefaultAutoCommit = con.getAutoCommit();
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "determine auto-commit behaviour", JdbcActionType.OTHER);
        }

        try {
            DatabaseMetaData metaData = con.getMetaData();

            supportsBatchUpdates = supportsBatchUpdates(metaData);

            try {
                identifierQuoteString = metaData.getIdentifierQuoteString();
                if (identifierQuoteString != null) {
                    identifierQuoteString = identifierQuoteString.trim();
                }
            } catch (SQLException e) {
                logger.warn("could not retrieve identifier quote string from database metadata", e);
            }

            usesCatalogsAsSchemas = usesCatalogsAsSchemas(metaData);
            try {
                databaseProductName = metaData.getDatabaseProductName();
                databaseVersion = metaData.getDatabaseProductVersion();
            } catch (SQLException e) {
                logger.warn("Could not retrieve metadata: " + e.getMessage());
            }
        } catch (SQLException e) {
            logger.debug("Unexpected exception during JdbcDataContext initialization", e);
        } finally {
            closeIfNecessary(con);
        }
        _databaseProductName = databaseProductName;
        _databaseVersion = databaseVersion;
        logger.debug("Database product name: {}", _databaseProductName);
        if (DATABASE_PRODUCT_MYSQL.equals(_databaseProductName)) {
            setQueryRewriter(new MysqlQueryRewriter(this));
        } else if (DATABASE_PRODUCT_POSTGRESQL.equals(_databaseProductName)) {
            setQueryRewriter(new PostgresqlQueryRewriter(this));
        } else if (DATABASE_PRODUCT_ORACLE.equals(_databaseProductName)) {
            setQueryRewriter(new OracleQueryRewriter(this));
        } else if (DATABASE_PRODUCT_SQLSERVER.equals(_databaseProductName)) {
            setQueryRewriter(new SQLServerQueryRewriter(this));
        } else if (DATABASE_PRODUCT_DB2.equals(_databaseProductName)
                || (_databaseProductName != null && _databaseProductName.startsWith(DATABASE_PRODUCT_DB2_PREFIX))) {
            setQueryRewriter(new DB2QueryRewriter(this));
        } else if (DATABASE_PRODUCT_HSQLDB.equals(_databaseProductName)) {
            setQueryRewriter(new HsqldbQueryRewriter(this));
        } else if (DATABASE_PRODUCT_H2.equals(_databaseProductName)) {
            setQueryRewriter(new H2QueryRewriter(this));
        } else if (DATABASE_PRODUCT_HIVE.equals(_databaseProductName)) {
            int majorVersion = -1;
            String[] parts = databaseVersion.split("\\.");
            if (parts.length < 2) {
                logger.warn("Illegal Hive Version: " + databaseVersion + " (expected A.B.* format)");
            } else {
                try {
                    majorVersion = Integer.valueOf(parts[0]);
                } catch (NumberFormatException ne) {
                    logger.warn("Illegal Hive Major Version: " + databaseVersion + " (can not get major version)");
                }
            }
            if (majorVersion == -1) {
                logger.warn("Illegal Hive Major Version :" + majorVersion + " can not decide which QueryRewrite to use, use DefaultQueryRewriter");
                setQueryRewriter(new DefaultQueryRewriter(this));
            } else if (majorVersion == 0 || majorVersion == 1) {
                //version 0 or 1 use HiveQueryRewriter
                setQueryRewriter(new HiveQueryRewriter(this));
            } else if (majorVersion >= 2) {
                //from version 2 support limit offset
                setQueryRewriter(new Hive2QueryRewriter(this));
            }
        } else if (DATABASE_PRODUCT_SQLITE.equals(_databaseProductName)) {
            setQueryRewriter(new SQLiteQueryRewriter(this));
        } else if (DATABASE_PRODUCT_IMPALA.equals(_databaseProductName)) {
            setQueryRewriter(new ImpalaQueryRewriter(this));
        }else {
            setQueryRewriter(new DefaultQueryRewriter(this));
        }

        _supportsBatchUpdates = supportsBatchUpdates;
        _identifierQuoteString = identifierQuoteString;
        _usesCatalogsAsSchemas = usesCatalogsAsSchemas;
        _metadataLoader = new JdbcMetadataLoader(this, _usesCatalogsAsSchemas, _identifierQuoteString);
    }

    /**
     * Creates the strategy based on a {@link Connection}
     * 
     * @param connection the database connection
     */
    public JdbcDataContext(Connection connection) {
        this(connection, TableType.DEFAULT_TABLE_TYPES, null);
    }
    public JdbcDataContext(Connection connection, String defaultSchemaName){
        this(connection, TableType.DEFAULT_TABLE_TYPES, null);
        this._defaultSchemaName = defaultSchemaName;
    }

    /**
     * Creates the strategy based on a {@link DataSource}
     * 
     * @param dataSource the data source
     */
    public JdbcDataContext(DataSource dataSource) {
        this(dataSource, TableType.DEFAULT_TABLE_TYPES, null);
    }

    private boolean supportsBatchUpdates(DatabaseMetaData metaData) {
        if ("true".equals(System.getProperty(SYSTEM_PROPERTY_BATCH_UPDATES))) {
            return true;
        }
        if ("false".equals(System.getProperty(SYSTEM_PROPERTY_BATCH_UPDATES))) {
            return false;
        }

        try {
            return metaData.supportsBatchUpdates();
        } catch (Exception e) {
            logger.warn("Could not determine if driver support batch updates, returning false", e);
            return false;
        }
    }

    private boolean usesCatalogsAsSchemas(DatabaseMetaData metaData) {
        boolean result = true;
        ResultSet rs = null;
        try {
            rs = metaData.getSchemas();
            while (rs.next() && result) {
                result = false;
            }
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "retrieve schema and catalog metadata", JdbcActionType.METADATA);
        } finally {
            FileHelper.safeClose(rs);
        }
        return result;
    }

    @Override
    public CompiledQuery compileQuery(Query query) {
        return new JdbcCompiledQuery(this, query);
    }

    @Override
    public DataSet executeQuery(CompiledQuery compiledQuery, Object... values) {

        final JdbcCompiledQuery jdbcCompiledQuery = (JdbcCompiledQuery) compiledQuery;

        final Query query = jdbcCompiledQuery.getQuery();
        final int countMatches = jdbcCompiledQuery.getParameters().size();

        final int valueArrayLength = values.length;

        if (countMatches != valueArrayLength) {
            throw new MetaModelException("Number of parameters in query and number of values does not match.");
        }

        final JdbcCompiledQueryLease lease = jdbcCompiledQuery.borrowLease();
        final DataSet dataSet;
        try {
            dataSet =
                    execute(lease.getConnection(), query, lease.getStatement(), jdbcCompiledQuery, lease, values, true);
        } catch (SQLException e) {
            // only close in case of an error - the JdbcDataSet will close
            // otherwise
            jdbcCompiledQuery.returnLease(lease);
            throw JdbcUtils.wrapException(e, "execute compiled query", JdbcActionType.QUERY);
        } catch (RuntimeException e) {
            // only close in case of an error - the JdbcDataSet will close
            // otherwise
            jdbcCompiledQuery.returnLease(lease);
            throw e;
        }

        return dataSet;
    }

    @SuppressWarnings("resource")
    private DataSet execute(Connection connection, Query query, Statement statement, JdbcCompiledQuery compiledQuery,
            JdbcCompiledQueryLease lease, Object[] values, boolean closeConnectionOnCloseDataSet)
            throws SQLException, MetaModelException {
        Integer maxRows = query.getMaxRows();

        final List<SelectItem> selectItems = query.getSelectClause().getItems();
        if (maxRows != null && maxRows.intValue() == 0) {
            return new EmptyDataSet(selectItems);
        }

        if (MetaModelHelper.containsNonSelectScalaFunctions(query)) {
            throw new MetaModelException(
                    "Scalar functions outside of SELECT clause is not supported for JDBC databases. Query rejected: "
                            + query);
        }

        for (SelectItem selectItem : selectItems) {
            final AggregateFunction aggregateFunction = selectItem.getAggregateFunction();
            if (aggregateFunction != null && !_queryRewriter.isAggregateFunctionSupported(aggregateFunction)) {
                throw new MetaModelException("Aggregate function '" + aggregateFunction.getFunctionName()
                        + "' is not supported on this JDBC database. Query rejected: " + query);
            }
        }

        if (_databaseProductName.equals(DATABASE_PRODUCT_POSTGRESQL)) {
            try {
                // this has to be done in order to make a result set not load
                // all data in memory only for Postgres.
                connection.setAutoCommit(false);
            } catch (Exception e) {
                logger.warn("Could not disable auto-commit (PostgreSQL specific hack)", e);
            }
        }

        ResultSet resultSet = null;

        // build a list of select items whose scalar functions has to be
        // evaluated client-side
        final List<SelectItem> scalarFunctionSelectItems = MetaModelHelper.getScalarFunctionSelectItems(selectItems);
        for (Iterator<SelectItem> it = scalarFunctionSelectItems.iterator(); it.hasNext();) {
            final SelectItem selectItem = (SelectItem) it.next();
            if (_queryRewriter.isScalarFunctionSupported(selectItem.getScalarFunction())) {
                it.remove();
            }
        }

        final Integer firstRow = query.getFirstRow();
        boolean postProcessFirstRow = false;
        if (firstRow != null) {
            if (_queryRewriter.isFirstRowSupported(query)) {
                logger.debug("First row property will be treated by query rewriter");
            } else {
                postProcessFirstRow = true;
            }
        }

        boolean postProcessMaxRows = false;
        if (maxRows != null) {
            if (postProcessFirstRow) {
                // if First row is being post processed, we need to
                // increment the "Max rows" accordingly (but subtract one, since
                // firstRow is 1-based).
                maxRows = maxRows + (firstRow - 1);
                query = query.clone().setMaxRows(maxRows);

                logger.debug("Setting Max rows to {} because of post processing strategy of First row.", maxRows);
            }

            if (_queryRewriter.isMaxRowsSupported()) {
                logger.debug("Max rows property will be treated by query rewriter");
            } else {
                try {
                    statement.setMaxRows(maxRows);
                } catch (SQLException e) {
                    if (logger.isInfoEnabled()) {
                        logger.info("setMaxRows(" + maxRows + ") was rejected.", e);
                    }
                    postProcessMaxRows = true;
                }
            }
        }

        DataSet dataSet = null;
        try {
            final int fetchSize = getFetchSize(query, statement);

            logger.debug("Applying fetch_size={}", fetchSize);

            try {
                statement.setFetchSize(fetchSize);
            } catch (Exception e) {
                // Ticket #372: Sometimes an exception is thrown here even
                // though it's contrary to the jdbc spec. We'll proceed without
                // doing anything about it though.
                logger.info("Could not get or set fetch size on Statement: {}", e.getMessage());
            }

            if (lease == null) {
                final String queryString = _queryRewriter.rewriteQuery(query);

                logger.debug("Executing rewritten query: {}", queryString);

                resultSet = statement.executeQuery(queryString);
            } else {
                PreparedStatement preparedStatement = (PreparedStatement) statement;
                for (int i = 0; i < values.length; i++) {
                    preparedStatement.setObject(i + 1, values[i]);
                }
                resultSet = preparedStatement.executeQuery();
            }
            try {
                resultSet.setFetchSize(fetchSize);
            } catch (Exception e) {
                logger.warn("Could not set fetch size on ResultSet: {}", e.getMessage());
            }

            if (postProcessFirstRow) {
                // we iterate to the "first row" using the resultset itself.
                for (int i = 1; i < firstRow; i++) {
                    if (!resultSet.next()) {
                        // the result set was not as long as the first row
                        if (resultSet != null) {
                            resultSet.close();
                        }
                        return new EmptyDataSet(selectItems);
                    }
                }
            }

            if (lease == null) {
                dataSet = new JdbcDataSet(query, this, connection, statement, resultSet, closeConnectionOnCloseDataSet);
            } else {
                dataSet = new JdbcDataSet(compiledQuery, lease, resultSet);
            }

            if (postProcessMaxRows) {
                dataSet = new MaxRowsDataSet(dataSet, maxRows);
            }

            if (!scalarFunctionSelectItems.isEmpty()) {
                dataSet = new ScalarFunctionDataSet(scalarFunctionSelectItems, dataSet);
                dataSet = MetaModelHelper.getSelection(selectItems, dataSet);
            }

        } catch (SQLException exception) {
            if (resultSet != null) {
                resultSet.close();
            }
            throw exception;
        }
        return dataSet;
    }

    @Override
    public DataSet executeQuery(Query query) throws MetaModelException {
        final Connection connection = getConnection();
        return executeQuery(connection, query, true);
    }

    protected DataSet executeQuery(Connection connection, Query query, boolean closeConnectionOnCloseDataSet) {
        final Statement statement;
        try {
            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "create statement for query", JdbcActionType.OTHER);
        }

        final DataSet dataSet;
        try {
            dataSet = execute(connection, query, statement, null, null, null, closeConnectionOnCloseDataSet);
        } catch (SQLException e) {
            // only close in case of an error - the JdbcDataSet will close
            // otherwise
            close(connection);
            throw JdbcUtils.wrapException(e, "execute query", JdbcActionType.QUERY);
        } catch (RuntimeException e) {
            // only close in case of an error - the JdbcDataSet will close
            // otherwise
            close(connection);
            throw e;
        }

        return dataSet;
    }

    private int getFetchSize(Query query, final Statement statement) {
        try {
            final int defaultFetchSize = statement.getFetchSize();
            if (DATABASE_PRODUCT_MYSQL.equals(_databaseProductName) && defaultFetchSize == Integer.MIN_VALUE) {
                return defaultFetchSize;
            }
        } catch (Exception e) {
            // exceptions here are ignored.
            logger.debug("Ignoring exception while getting fetch size", e);
        }
        return _fetchSizeCalculator.getFetchSize(query);
    }

    /**
     * Quietly closes the connection
     * 
     * @param connection The connection to close (if it makes sense, @see closeIfNecessary)
     * 
     */
    public void close(Connection connection) {
        closeIfNecessary(connection);
    }

    /**
     * Convenience method to get the available catalogNames using this connection.
     * 
     * @return a String-array with the names of the available catalogs.
     */
    public String[] getCatalogNames() {
        Connection connection = getConnection();

        // Retrieve metadata
        DatabaseMetaData metaData = null;
        ResultSet rs = null;
        try {
            metaData = connection.getMetaData();
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "retrieve metadata", JdbcActionType.METADATA);
        }

        // Retrieve catalogs
        logger.debug("Retrieving catalogs");
        List<String> catalogs = new ArrayList<>();
        try {
            rs = metaData.getCatalogs();
            while (rs.next()) {
                String catalogName = rs.getString(1);
                logger.debug("Found catalogName: {}", catalogName);
                catalogs.add(catalogName);
            }
        } catch (SQLException e) {
            logger.error("Error retrieving catalog metadata", e);
        } finally {
            FileHelper.safeClose(rs);
            close(connection);
            logger.debug("Retrieved {} catalogs", catalogs.size());
        }
        return catalogs.toArray(new String[catalogs.size()]);
    }

    /**
     * Gets the delegate from the JDBC API (ie. Connection or DataSource) that is being used to perform database
     * interactions.
     * 
     * @return either a DataSource or a Connection, depending on the configuration of the DataContext.
     */
    public Object getDelegate() {
        if (_dataSource == null) {
            return _connection;
        }
        return _dataSource;
    }

    /**
     * Gets an appropriate connection object to use - either a dedicated connection or a new connection from the
     * datasource object.
     * 
     * Hint: Use the {@link #close(Connection)} method to close the connection (and any ResultSet or Statements
     * involved).
     */
    public Connection getConnection() {
        if (_dataSource == null) {
            return _connection;
        }
        try {
            return _dataSource.getConnection();
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "establish connection", JdbcActionType.OTHER);
        }
    }

    private void closeIfNecessary(Connection con) {
        if (con != null) {
            if (_dataSource != null) {
                // closing connections after individual usage is only necessary
                // when they are being pulled from a DataSource.
                FileHelper.safeClose(con);
            }
        }
    }

    @Override
    public String getDefaultSchemaName() {
        if(this._defaultSchemaName != null){
            return this._defaultSchemaName;
        }
        // Use a boolean to check if the result has been
        // found, because a schema name can actually be
        // null (for example in the case of Firebird
        // databases).
        boolean found = false;
        String result = null;
        List<String> schemaNames = getSchemaNames();

        // First strategy: If there's only one schema available, that must
        // be it
        if (schemaNames.size() == 1) {
            result = schemaNames.get(0);
            found = true;
        }

        if (!found) {
            Connection connection = getConnection();
            try {
                DatabaseMetaData metaData = connection.getMetaData();

                // Second strategy: Find default schema name by examining the
                // URL
                if (!found) {
                    String url = null;
                    try {
                        url = metaData.getURL();
                    } catch (SQLException e) {
                        if (!DATABASE_PRODUCT_HIVE.equals(_databaseProductName)) {
                            throw e;
                        }
                    }
                    if (url != null && url.length() > 0) {
                        if (schemaNames.size() > 0) {
                            StringTokenizer st = new StringTokenizer(url, "/\\:");
                            int tokenCount = st.countTokens();
                            if (tokenCount > 0) {
                                for (int i = 1; i < tokenCount; i++) {
                                    st.nextToken();
                                }
                                String lastToken = st.nextToken();
                                if (lastToken.indexOf("?") != -1) {
                                    lastToken = lastToken.substring(0, lastToken.indexOf("?"));
                                }
                                // Sorting schema names in order of decreasing length

                                Collections.sort(schemaNames, new Comparator<String>() {
                                    @Override
                                    public int compare(String s1, String s2) {
                                        return s2.length() - s1.length();
                                    }
                                });
                                for (int i = 0; i < schemaNames.size() && !found; i++) {
                                    String schemaName = schemaNames.get(i);
                                    if (lastToken.indexOf(schemaName) != -1) {
                                        result = schemaName;
                                        found = true;
                                    }
                                }
                            }
                        }
                    }
                }

                // Third strategy: Check for schema equal to username
                if (!found) {
                    String username = null;
                    try {
                        username = metaData.getUserName();
                    } catch (SQLException e) {
                        if (!DATABASE_PRODUCT_HIVE.equals(_databaseProductName)) {
                            throw e;
                        }
                    }
                    if (username != null) {
                        for (int i = 0; i < schemaNames.size() && !found; i++) {
                            if (username.equalsIgnoreCase(schemaNames.get(i))) {
                                result = schemaNames.get(i);
                                found = true;
                            }
                        }
                    }
                }

            } catch (SQLException e) {
                throw JdbcUtils.wrapException(e, "determine default schema name", JdbcActionType.METADATA);
            } finally {
                closeIfNecessary(connection);
            }

            // Fourth strategy: Find default schema name by vendor-specific
            // hacks
            if (!found) {
                if (DATABASE_PRODUCT_POSTGRESQL.equalsIgnoreCase(_databaseProductName)) {
                    if (_catalogName == null) {
                        result = "public";
                    } else {
                        result = _catalogName;
                    }
                    found = true;
                }
                if (DATABASE_PRODUCT_HSQLDB.equalsIgnoreCase(_databaseProductName)) {
                    result = findDefaultSchema("PUBLIC", schemaNames);
                }
                if (DATABASE_PRODUCT_SQLSERVER.equals(_databaseProductName)) {
                    result = findDefaultSchema(DEFAULT_SCHEMA_NAME_SQLSERVER, schemaNames);
                }
                if (DATABASE_PRODUCT_HIVE.equals(_databaseProductName)) {
                    result = findDefaultSchema("default", schemaNames);
                }
            }
        }
        return result;
    }

    private String findDefaultSchema(final String defaultName, final List<String> schemaNames) {
        for (String schemaName : schemaNames) {
            if (defaultName.equals(schemaName)) {
                return schemaName;
            }
        }
        return null;
    }

    /**
     * Microsoft SQL Server returns users instead of schemas when calling metadata.getSchemas() This is a simple
     * workaround.
     * 
     * @return
     * @throws SQLException
     */
    private Set<String> getSchemaSQLServerNames(DatabaseMetaData metaData) throws SQLException {
        // Distinct schema names. metaData.getTables() is a denormalized resultset
        final Set<String> schemas = new HashSet<>();

        // Add the default schema (if present) even though it can have no tables
        if (hasDefaultSQLServerSchema(metaData)) {
            schemas.add(DEFAULT_SCHEMA_NAME_SQLSERVER);
        }

        // Find other schemas from tables
        try (ResultSet tables = metaData
                .getTables(_catalogName, null, null, JdbcUtils.getTableTypesAsStrings(_tableTypes))) {
            while (tables.next()) {
                schemas.add(tables.getString(SCHEMA_NAME_IDENTIFIER));
            }
        }

        return schemas;
    }

    private static boolean hasDefaultSQLServerSchema(final DatabaseMetaData metaData) throws SQLException {
        try (ResultSet schemas = metaData.getSchemas()) {
            while (schemas.next()) {
                if (schemas.getString(SCHEMA_NAME_IDENTIFIER).equals(DEFAULT_SCHEMA_NAME_SQLSERVER)) {
                    return true;
                }
            }
        }
        return false;
    }

    public JdbcDataContext setQueryRewriter(IQueryRewriter queryRewriter) {
        if (queryRewriter == null) {
            throw new IllegalArgumentException("Query rewriter cannot be null");
        }
        _queryRewriter = queryRewriter;
        return this;
    }

    public IQueryRewriter getQueryRewriter() {
        return _queryRewriter;
    }

    public String getIdentifierQuoteString() {
        return _identifierQuoteString;
    }

    @Override
    protected List<String> getSchemaNamesInternal() {
        Connection connection = getConnection();
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            List<String> result = new ArrayList<>();

            if (DATABASE_PRODUCT_SQLSERVER.equals(_databaseProductName)) {
                result = new ArrayList<>(getSchemaSQLServerNames(metaData));
            } else if (_usesCatalogsAsSchemas) {
                String[] catalogNames = getCatalogNames();
                for (String name : catalogNames) {
                    logger.debug("Found catalogName: {}", name);
                    result.add(name);
                }
            } else {
                ResultSet rs = null;
                try {
                    rs = metaData.getSchemas();
                    while (rs.next()) {
                        String schemaName = rs.getString(1);
                        logger.debug("Found schemaName: {}", schemaName);
                        result.add(schemaName);
                    }
                } finally {
                    FileHelper.safeClose(rs);
                }
            }

            if (DATABASE_PRODUCT_MYSQL.equals(_databaseProductName)) {
                result.remove("information_schema");
            }

            // If still no schemas are found, add a schema with a null-name
            if (result.isEmpty()) {
                logger.info("No schemas or catalogs found. Creating unnamed schema.");
                result.add(null);
            }
            return result;
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "get schema names", JdbcActionType.METADATA);
        } finally {
            closeIfNecessary(connection);
        }
    }

    @Override
    protected Schema getSchemaByNameInternal(String name) {
        final JdbcSchema schema = new JdbcSchema(name, _metadataLoader);
        final Connection connection = getConnection();
        try {
            _metadataLoader.loadTables(schema, connection);
        } finally {
            close(connection);
        }
        return schema;
    }

    public FetchSizeCalculator getFetchSizeCalculator() {
        return _fetchSizeCalculator;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws RolledBackUpdateException when a potentially retryable {@link SQLException} was thrown during the
     *             execution of the update script.
     */
    @Override
    public UpdateSummary executeUpdate(final UpdateScript update) throws RolledBackUpdateException {
        final JdbcUpdateCallback updateCallback;

        if (_supportsBatchUpdates && update instanceof BatchUpdateScript) {
            updateCallback = new JdbcBatchUpdateCallback(this);
        } else {
            updateCallback = new JdbcSimpleUpdateCallback(this);
        }

        try {
            if (isSingleConnection() && isDefaultAutoCommit()) {
                // if auto-commit is going to be switched off and on during
                // updates, then the update needs to be synchronized, to avoid
                // race-conditions when switching off and on.
                synchronized (_connection) {
                    update.run(updateCallback);
                }
            } else {
                update.run(updateCallback);
            }
            updateCallback.close(true);
        } catch (UncheckedSQLException e) {
            updateCallback.close(false);
            throw new RolledBackUpdateException(e.getCause());
        } catch (RuntimeException e) {
            updateCallback.close(false);
            throw e;
        }

        return updateCallback.getUpdateSummary();
    }

    protected boolean isSingleConnection() {
        return _singleConnection;
    }

    protected boolean isDefaultAutoCommit() {
        return _isDefaultAutoCommit;
    }

    @Override
    protected boolean isQualifiedPathDelim(char c) {
        if (_identifierQuoteString == null || _identifierQuoteString.length() == 0) {
            return super.isQualifiedPathDelim(c);
        }
        return c == '.' || c == _identifierQuoteString.charAt(0);
    }

    public TableType[] getTableTypes() {
        return _tableTypes;
    }

    public String getCatalogName() {
        return _catalogName;
    }

    public String getDatabaseProductName() {
        return _databaseProductName;
    }

    public String getDatabaseVersion() {
        return _databaseVersion;
    }
}
