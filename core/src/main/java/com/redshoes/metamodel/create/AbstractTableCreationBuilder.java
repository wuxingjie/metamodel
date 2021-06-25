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
package com.redshoes.metamodel.create;

import com.redshoes.metamodel.MetaModelHelper;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.MutableColumn;
import com.redshoes.metamodel.schema.MutableTable;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.schema.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Abstract {@link TableCreationBuilder} implementation, provided as convenience
 * for {@link TableCreatable} implementations. Handles all the building
 * operations, but not the commit operation.
 */
public abstract class AbstractTableCreationBuilder<U extends UpdateCallback> implements TableCreationBuilder {

    private static final Logger logger = LoggerFactory.getLogger(AbstractTableCreationBuilder.class);

    private final U _updateCallback;
    private final MutableTable _table;
    private final Schema _schema;

    public AbstractTableCreationBuilder(U updateCallback, Schema schema, String name) {
        if (schema != null && schema.getTableByName(name) != null) {
            throw new IllegalArgumentException("A table with the name '" + name + "' already exists in schema: "
                    + schema);
        }
        _updateCallback = updateCallback;
        _schema = MetaModelHelper.resolveUnderlyingSchema(schema);
        _table = new MutableTable(name, TableType.TABLE, schema);
    }

    protected U getUpdateCallback() {
        return _updateCallback;
    }

    protected Schema getSchema() {
        return _schema;
    }

    protected MutableTable getTable() {
        return _table;
    }

    @Override
    public Table toTable() {
        return _table;
    }

    @Override
    public TableCreationBuilder like(Table table) {
        logger.debug("like({})", table);
        List<Column> columns = table.getColumns();
        for (Column column : columns) {
            withColumn(column.getName()).like(column);
        }
        return this;
    }

    @Override
    public ColumnCreationBuilder withColumn(String name) {
        logger.debug("withColumn({})", name);
        MutableColumn col = (MutableColumn) _table.getColumnByName(name);
        if (col == null) {
            col = new MutableColumn(name).setTable(_table).setColumnNumber(_table.getColumnCount());
            _table.addColumn(col);
        }
        return new ColumnCreationBuilderImpl(this, col);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        sb.append(_table.getQualifiedLabel());
        sb.append(" (");
        List<Column> columns = _table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(',');
            }
            Column column = columns.get(i);
            sb.append(column.getName());
            ColumnType type = column.getType();
            if (type != null) {
                sb.append(' ');
                sb.append(type.toString());
                final String columnSizeContent =  getSizeContent(column.getColumnSize(),column.getDecimalDigits());
                if(columnSizeContent != null ){
                    sb.append('(');
                    sb.append(columnSizeContent);
                    sb.append(')');
                }
            }
            if (column.isNullable() != null
                    && !column.isNullable().booleanValue()) {
                sb.append(" NOT NULL");
            }
        }
        boolean primaryKeyExists = false;
        for(int i = 0 ; i < columns.size() ; i++) {
            if(columns.get(i).isPrimaryKey()) {
                if(!primaryKeyExists) {
                    sb.append(", PRIMARY KEY(");
                    sb.append(columns.get(i).getName());
                    primaryKeyExists = true;
                } else {
                    sb.append(",");
                    sb.append(columns.get(i).getName());
                }
            }    
        }
        if(primaryKeyExists) {
	        sb.append(")");
        }
        sb.append(")");
        return sb.toString();
    }
     private String getSizeContent(Integer columnSize,Integer decimalDigits){
        if(columnSize == null){
            return null;
        }
        StringBuilder sizeContent = new StringBuilder();
        sizeContent.append(columnSize.intValue());
        if (decimalDigits != null) {
            sizeContent.append(",");
            sizeContent.append(decimalDigits.intValue());
        }
        return sizeContent.toString();
    }
}
