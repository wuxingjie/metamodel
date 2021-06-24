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
package com.redshoes.metamodel.hbase;

import java.io.IOException;
import java.util.List;

import com.redshoes.metamodel.annotations.InterfaceStability;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.data.DataSetHeader;
import com.redshoes.metamodel.data.Row;
import com.redshoes.metamodel.data.SimpleDataSetHeader;
import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.MutableSchema;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.util.FileHelper;
import com.redshoes.metamodel.util.SimpleTableDef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.QueryPostprocessDataContext;
import com.redshoes.metamodel.UpdateScript;
import com.redshoes.metamodel.UpdateSummary;
import com.redshoes.metamodel.UpdateableDataContext;

/**
 * MetaModel adaptor for Apache HBase.
 */
@InterfaceStability.Evolving
public class HBaseDataContext extends QueryPostprocessDataContext implements UpdateableDataContext {

    public static final String FIELD_ID = "_id";

    private final HBaseConfiguration _configuration;
    private final Connection _connection;

    /**
     * Creates a {@link HBaseDataContext}.
     * 
     * @param configuration
     */
    public HBaseDataContext(HBaseConfiguration configuration) {
        super(false);
        Configuration config = createConfig(configuration);
        _configuration = configuration;
        _connection = createConnection(config);
    }

    /**
     * Creates a {@link HBaseDataContext}.
     * 
     * @param configuration
     * @param connection
     */
    public HBaseDataContext(HBaseConfiguration configuration, Connection connection) {
        super(false);
        _configuration = configuration;
        _connection = connection;
    }

    private Connection createConnection(Configuration config) {
        try {
            return ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
    }

    private static Configuration createConfig(HBaseConfiguration configuration) {
        Configuration config = org.apache.hadoop.hbase.HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", configuration.getZookeeperHostname());
        config.set("hbase.zookeeper.property.clientPort", Integer.toString(configuration.getZookeeperPort()));
        config.set("hbase.client.retries.number", Integer.toString(configuration.getHBaseClientRetries()));
        config.set("zookeeper.session.timeout", Integer.toString(configuration.getZookeeperSessionTimeout()));
        config.set("zookeeper.recovery.retry", Integer.toString(configuration.getZookeeperRecoveryRetries()));
        return config;
    }

    /**
     * Gets the {@link Admin} used by this {@link DataContext}
     * 
     * @return
     */
    public Admin getAdmin() {
        try {
            return _connection.getAdmin();
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
    }

    public Connection getConnection() {
        return _connection;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema schema = new MutableSchema(_configuration.getSchemaName());

        SimpleTableDef[] tableDefinitions = _configuration.getTableDefinitions();
        if (tableDefinitions == null) {
            try {
                final List<TableDescriptor> tables = getAdmin().listTableDescriptors();
                tableDefinitions = new SimpleTableDef[tables.size()];
                for (int i = 0; i < tables.size(); i++) {
                    final String tableName = tables.get(i).getTableName().getNameAsString();
                    final SimpleTableDef emptyTableDef = new SimpleTableDef(tableName, new String[0]);
                    tableDefinitions[i] = emptyTableDef;
                }
            } catch (IOException e) {
                throw new MetaModelException(e);
            }
        }

        for (SimpleTableDef tableDef : tableDefinitions) {
            schema.addTable(new HBaseTable(this, tableDef, schema, _configuration.getDefaultRowKeyType()));
        }

        return schema;
    }

    /**
     * Gets the {@link HBaseConfiguration} that is used in this datacontext.
     * 
     * @return
     */
    public HBaseConfiguration getConfiguration() {
        return _configuration;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _configuration.getSchemaName();
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        if (whereItems != null && !whereItems.isEmpty()) {
            return null;
        }

        long result = 0;
        final org.apache.hadoop.hbase.client.Table hTable = getHTable(table.getName());
        try {
            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner scanner = hTable.getScanner(scan);
            try {
                while (scanner.next() != null) {
                    result++;
                }
            } finally {
                scanner.close();
            }
            return result;
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
    }

    protected org.apache.hadoop.hbase.client.Table getHTable(String name) {
        try {
            final TableName tableName = TableName.valueOf(name);
            final org.apache.hadoop.hbase.client.Table hTable = _connection.getTable(tableName);
            return hTable;
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
    }

    @Override
    protected Row executePrimaryKeyLookupQuery(Table table, List<SelectItem> selectItems, Column primaryKeyColumn,
                                               Object keyValue) {
        final org.apache.hadoop.hbase.client.Table hTable = getHTable(table.getName());
        final Get get = new Get(ByteUtils.toBytes(keyValue));
        try {
            final Result result = hTable.get(get);
            final DataSetHeader header = new SimpleDataSetHeader(selectItems);
            final Row row = new HBaseRow(header, result);
            return row;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to execute HBase get operation with " + primaryKeyColumn.getName()
                    + " = " + keyValue, e);
        } finally {
            FileHelper.safeClose(hTable);
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        final Scan scan = new Scan();
        for (Column column : columns) {
            if (!column.isPrimaryKey()) {
                final int colonIndex = column.getName().indexOf(':');
                final int len = column.getName().length();
                if (colonIndex != -1) {
                    final String family = column.getName().substring(0, colonIndex);
                    final String colName = column.getName().substring(colonIndex + 1, len);
                    scan.addColumn(family.getBytes(), colName.getBytes());
                } else {
                    scan.addFamily(column.getName().getBytes());
                }
            }
        }

        if (maxRows > 0) {
            setMaxRows(scan, maxRows);
        }

        final org.apache.hadoop.hbase.client.Table hTable = getHTable(table.getName());
        try {
            final ResultScanner scanner = hTable.getScanner(scan);
            return new HBaseDataSet(columns, scanner, hTable);
        } catch (Exception e) {
            FileHelper.safeClose(hTable);
            throw new MetaModelException(e);
        }
    }

    private void setMaxRows(Scan scan, int maxRows) {
        scan.setFilter(new PageFilter(maxRows));
    }

    @Override
    public UpdateSummary executeUpdate(UpdateScript update) {
        final HBaseUpdateCallback callback = new HBaseUpdateCallback(this);
        update.run(callback);

        return callback.getUpdateSummary();
    }

    HBaseClient getHBaseClient() {
        return new HBaseClient(this.getConnection());
    }
}
