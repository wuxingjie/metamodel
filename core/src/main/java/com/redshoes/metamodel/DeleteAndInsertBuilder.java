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

import java.util.List;
import java.util.ListIterator;

import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.data.DefaultRow;
import com.redshoes.metamodel.data.Row;
import com.redshoes.metamodel.data.SimpleDataSetHeader;
import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.update.AbstractRowUpdationBuilder;
import com.redshoes.metamodel.update.RowUpdationBuilder;

/**
 * Simple implementation of the {@link RowUpdationBuilder} interface, which
 * simply uses a combined delete+insert strategy for performing updates. Note
 * that this implementation is not desirable performance-wise in many cases, but
 * does provide a functional equivalent to a "real" update.
 */
public class DeleteAndInsertBuilder extends AbstractRowUpdationBuilder {

    private final AbstractUpdateCallback _updateCallback;

    public DeleteAndInsertBuilder(AbstractUpdateCallback updateCallback, Table table) {
        super(table);
        assert updateCallback.isInsertSupported();
        assert updateCallback.isDeleteSupported();
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        // retain rows in memory
        List<Row> rows = getRowsToUpdate();

        // delete rows
        _updateCallback.deleteFrom(getTable()).where(getWhereItems()).execute();

        // modify rows
        rows = updateRows(rows);

        // insert rows
        for (Row row : rows) {
            _updateCallback.insertInto(getTable()).like(row).execute();
        }
    }

    private List<Row> updateRows(List<Row> rows) {
        for (ListIterator<Row> it = rows.listIterator(); it.hasNext();) {
            final Row original = (Row) it.next();
            final Row updated = update(original);
            it.set(updated);
        }
        return rows;
    }

    /**
     * Produces an updated row out of the original
     * 
     * @param original
     * @return
     */
    private Row update(final Row original) {
        List<SelectItem> items = original.getSelectItems();
        Object[] values = new Object[items.size()];
        for (int i = 0; i < items.size(); i++) {
            final Object value;
            Column column = items.get(i).getColumn();
            if (isSet(column)) {
                // use update statement's value
                value = getValues()[i];
            } else {
                // use original value
                value = original.getValue(i);
            }
            values[i] = value;
        }
        return new DefaultRow(new SimpleDataSetHeader(items), values);
    }

    protected List<Row> getRowsToUpdate() {
        final DataContext dc = _updateCallback.getDataContext();
        final Table table = getTable();
        final List<FilterItem> whereItems = getWhereItems();
        try (DataSet dataSet = dc.query().from(table).select(table.getColumns()).where(whereItems).execute()) {
            return dataSet.toRows();
        }
    }

}
