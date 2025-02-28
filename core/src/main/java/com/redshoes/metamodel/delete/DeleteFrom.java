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
package com.redshoes.metamodel.delete;

import java.util.ArrayList;
import java.util.List;

import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.UpdateScript;
import com.redshoes.metamodel.UpdateableDataContext;
import com.redshoes.metamodel.data.WhereClauseBuilder;
import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.query.builder.AbstractFilterBuilder;
import com.redshoes.metamodel.query.builder.FilterBuilder;

/**
 * Represents a single DELETE FROM operation to be applied to a
 * {@link UpdateableDataContext}. Instead of providing a custom implementation
 * of the {@link UpdateScript} interface, one can use this pre-built delete from
 * implementation. Some {@link DataContext}s may even optimize specifically
 * based on the knowledge that there will only be a single delete from statement
 * executed.
 */
public final class DeleteFrom implements UpdateScript, WhereClauseBuilder<DeleteFrom> {

    private final List<FilterItem> _whereItems;
    private final Table _table;

    public DeleteFrom(Table table) {
        _table = table;
        _whereItems = new ArrayList<FilterItem>();
    }

    @Override
    public void run(UpdateCallback callback) {
        callback.deleteFrom(_table).where(_whereItems).execute();
    }

    @Override
    public FilterBuilder<DeleteFrom> where(Column column) {
        SelectItem selectItem = new SelectItem(column);
        return new AbstractFilterBuilder<DeleteFrom>(selectItem) {
            @Override
            protected DeleteFrom applyFilter(FilterItem filter) {
                return where(filter);
            }
        };
    }

    @Override
    public FilterBuilder<DeleteFrom> where(String columnName) {
        Column column = _table.getColumnByName(columnName);
        if (column == null) {
            throw new IllegalArgumentException("No such column: " + columnName);
        }
        return where(column);
    }

    @Override
    public DeleteFrom where(FilterItem... filterItems) {
        for (FilterItem filterItem : filterItems) {
            _whereItems.add(filterItem);
        }
        return this;
    }

    @Override
    public DeleteFrom where(Iterable<FilterItem> filterItems) {
        for (FilterItem filterItem : filterItems) {
            _whereItems.add(filterItem);
        }
        return this;
    }
}
