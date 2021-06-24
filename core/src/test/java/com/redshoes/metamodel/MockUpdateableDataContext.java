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
package com.redshoes.metamodel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.redshoes.metamodel.create.TableCreationBuilder;
import com.redshoes.metamodel.data.CachingDataSetHeader;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.data.DefaultRow;
import com.redshoes.metamodel.data.EmptyDataSet;
import com.redshoes.metamodel.data.InMemoryDataSet;
import com.redshoes.metamodel.data.Row;
import com.redshoes.metamodel.delete.AbstractRowDeletionBuilder;
import com.redshoes.metamodel.delete.RowDeletionBuilder;
import com.redshoes.metamodel.drop.TableDropBuilder;
import com.redshoes.metamodel.insert.AbstractRowInsertionBuilder;
import com.redshoes.metamodel.insert.RowInsertionBuilder;
import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.MutableColumn;
import com.redshoes.metamodel.schema.MutableSchema;
import com.redshoes.metamodel.schema.MutableTable;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;

public class MockUpdateableDataContext extends QueryPostprocessDataContext implements UpdateableDataContext {

    private final List<Object[]> _values = new ArrayList<Object[]>();

    private final MutableTable _table;
    private final MutableSchema _schema;
    
    public MockUpdateableDataContext() {
        this(true);
    }

    public MockUpdateableDataContext(boolean addDefaultTableAlias) {
        super(addDefaultTableAlias);
        _values.add(new Object[] { "1", "hello" });
        _values.add(new Object[] { "2", "there" });
        _values.add(new Object[] { "3", "world" });

        _table = new MutableTable("table");
        _table.addColumn(new MutableColumn("foo", ColumnType.VARCHAR).setTable(_table).setColumnNumber(0));
        _table.addColumn(new MutableColumn("bar", ColumnType.VARCHAR).setTable(_table).setColumnNumber(1));
        _schema = new MutableSchema("schema", _table);
        _table.setSchema(_schema);
    }

    public MutableTable getTable() {
        return _table;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        if (table != _table) {
            throw new IllegalArgumentException("Unknown table: " + table);
        }

        List<Row> rows = new ArrayList<Row>();
        List<SelectItem> items = columns.stream().map(SelectItem::new).collect(Collectors.toList());
        CachingDataSetHeader header = new CachingDataSetHeader(items);

        for (final Object[] values : _values) {
            Object[] rowValues = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                int columnNumber = columns.get(i).getColumnNumber();
                rowValues[i] = values[columnNumber];
            }
            rows.add(new DefaultRow(header, rowValues));
        }

        if (rows.isEmpty()) {
            return new EmptyDataSet(items);
        }
        return new InMemoryDataSet(header, rows);
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _schema.getName();
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        return _schema;
    }

    @Override
    public UpdateSummary executeUpdate(UpdateScript update) {
        final AbstractUpdateCallback callback = new AbstractUpdateCallback(this) {

            @Override
            public boolean isDeleteSupported() {
                return true;
            }

            @Override
            public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
                    UnsupportedOperationException {
                if (table != _table) {
                    throw new IllegalArgumentException("Unknown table: " + table);
                }
                return new AbstractRowDeletionBuilder(table) {
                    @Override
                    public void execute() throws MetaModelException {
                        delete(getWhereItems());
                    }
                };
            }

            @Override
            public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
                    UnsupportedOperationException {
                if (table != _table) {
                    throw new IllegalArgumentException("Unknown table: " + table);
                }
                return new AbstractRowInsertionBuilder<UpdateCallback>(this, table) {

                    @Override
                    public void execute() throws MetaModelException {
                        Object[] values = toRow().getValues();
                        _values.add(values);
                    }
                };
            }

            @Override
            public boolean isDropTableSupported() {
                return false;
            }

            @Override
            public boolean isCreateTableSupported() {
                return false;
            }

            @Override
            public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
                    UnsupportedOperationException {
                throw new UnsupportedOperationException();
            }

            @Override
            public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
                    IllegalStateException {
                throw new UnsupportedOperationException();
            }
        };
        
        update.run(callback);
        
        return callback.getUpdateSummary();
    }

    private void delete(List<FilterItem> whereItems) {
        final List<SelectItem> selectItems = _table.getColumns().stream().map(SelectItem::new).collect(Collectors.toList());
        final CachingDataSetHeader header = new CachingDataSetHeader(selectItems);
        for (Iterator<Object[]> it = _values.iterator(); it.hasNext();) {
            Object[] values = (Object[]) it.next();
            DefaultRow row = new DefaultRow(header, values);
            boolean delete = true;
            for (FilterItem filterItem : whereItems) {
                if (!filterItem.evaluate(row)) {
                    delete = false;
                    break;
                }
            }
            if (delete) {
                it.remove();
            }
        }
    }

    public List<Object[]> getValues() {
        return _values;
    }
}
