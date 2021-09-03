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
package com.redshoes.metamodel.insert;

import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.UpdateScript;
import com.redshoes.metamodel.UpdateableDataContext;
import com.redshoes.metamodel.data.AbstractRowBuilder;
import com.redshoes.metamodel.data.RowBuilder;
import com.redshoes.metamodel.data.Style;

/**
 * Represents a single INSERT INTO operation to be applied to a
 * {@link UpdateableDataContext}. Instead of providing a custom implementation
 * of the {@link UpdateScript} interface, one can use this pre-built
 * single-record insertion implementation. Some {@link DataContext}s may even
 * optimize specifically based on the knowledge that there will only be a single
 * record inserted.
 */
public final class InsertInto extends AbstractRowBuilder<InsertInto> implements UpdateScript, RowBuilder<InsertInto> {

    private final Table _table;

    public InsertInto(Table table) {
        super(table);
        _table = table;
    }

    @Override
    public void run(UpdateCallback callback) {
        RowInsertionBuilder insertBuilder = callback.insertInto(getTable());

        final Column[] columns = getColumns();
        final Object[] values = getValues();
        final Style[] styles = getStyles();
        final boolean[] explicitNulls = getExplicitNulls();

        for (int i = 0; i < columns.length; i++) {
            Object value = values[i];
            Column column = columns[i];
            Style style = styles[i];
            if (value == null) {
                if (explicitNulls[i]) {
                    insertBuilder = insertBuilder.value(column, value, style);
                }
            } else {
                insertBuilder = insertBuilder.value(column, value, style);
            }
        }

        insertBuilder.execute();
    }

    @Override
    public Table getTable() {
        return _table;
    }
}
