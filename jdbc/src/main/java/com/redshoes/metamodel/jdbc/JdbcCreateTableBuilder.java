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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.redshoes.metamodel.create.AbstractTableCreationBuilder;
import com.redshoes.metamodel.jdbc.dialects.IQueryRewriter;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.util.FileHelper;
import com.redshoes.metamodel.jdbc.JdbcUtils.JdbcActionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link } implementation that issues a SQL CREATE TABLE
 * statement.
 */
final class JdbcCreateTableBuilder extends AbstractTableCreationBuilder<JdbcUpdateCallback> {

    private static final Logger logger = LoggerFactory.getLogger(JdbcCreateTableBuilder.class);

    public JdbcCreateTableBuilder(JdbcUpdateCallback updateCallback, Schema schema, String name) {
        super(updateCallback, schema, name);
        if (!(schema instanceof JdbcSchema)) {
            throw new IllegalArgumentException("Not a valid JDBC schema: " + schema);
        }
    }

    @Override
    public Table execute() {
        final String sql = createSqlStatement();
        logger.info("Create table statement created: {}", sql);

        final Connection connection = getUpdateCallback().getConnection();
        Statement st = null;
        try {
            st = connection.createStatement();
            final int rowsAffected = st.executeUpdate(sql);
            logger.debug("Create table statement executed, {} rows affected", rowsAffected);
        } catch (SQLException e) {
            throw JdbcUtils.wrapException(e, "execute create table statement: " + sql, JdbcActionType.UPDATE);
        } finally {
            FileHelper.safeClose(st);
        }

        final JdbcSchema schema = (JdbcSchema) getSchema();
        schema.refreshTables(connection);
        return schema.getTableByName(getTable().getName());
    }

    protected String createSqlStatement() {
        return createSqlStatement(getTable());
    }

    private String createSqlStatement(Table table) {
        final IQueryRewriter queryRewriter = getUpdateCallback().getJdbcDataContext().getQueryRewriter();
        final StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        final Schema schema = getSchema();
        if (schema != null && schema.getName() != null) {
            sb.append(schema.getQualifiedLabel());
            sb.append(".");
        }
        sb.append(getUpdateCallback().quoteIfNescesary(table.getName()));
        sb.append(" (");
        final List<Column> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            final Column column = columns.get(i);
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(getUpdateCallback().quoteIfNescesary(column.getName()));
            sb.append(' ');
            final String nativeType = column.getNativeType();
            final Integer columnSize = column.getColumnSize();
            final Integer decimalDigits = column.getDecimalDigits();
            ColumnType columnType = column.getType();
            String columnTypeString = queryRewriter.rewriteColumnType(columnType, columnSize, decimalDigits);
            if (nativeType == null) {
                if (columnType == null) {
                    columnType = ColumnType.VARCHAR;
                    columnTypeString = queryRewriter.rewriteColumnType(columnType, columnSize, decimalDigits);
                }
                sb.append(columnTypeString);
            } else {
                sb.append(nativeType);
                if(columnSize != null){
                    sb.append('(');
                    sb.append(columnTypeString);
                    sb.append(')');
                }
            }
            if (column.isNullable() != null && !column.isNullable().booleanValue()) {
                sb.append(" NOT NULL");
            }
        }
        if (queryRewriter.isPrimaryKeySupported()) {
            boolean primaryKeyExists = false;
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).isPrimaryKey()) {
                    if (!primaryKeyExists) {
                        sb.append(", PRIMARY KEY(");
                        sb.append(columns.get(i).getName());
                        primaryKeyExists = true;
                    } else {
                        sb.append(",");
                        sb.append(columns.get(i).getName());
                    }
                }
            }
            if (primaryKeyExists) {
                sb.append(")");
            }
        }
        sb.append(")");

        return sb.toString();
    }

}
