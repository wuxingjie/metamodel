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
package com.redshoes.metamodel.intercept;

import com.redshoes.metamodel.AbstractUpdateCallback;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.create.TableCreationBuilder;
import com.redshoes.metamodel.delete.RowDeletionBuilder;
import com.redshoes.metamodel.drop.TableDropBuilder;
import com.redshoes.metamodel.insert.RowInsertionBuilder;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.update.RowUpdationBuilder;

/**
 * {@link UpdateCallback} wrapper that allows adding interceptors for certain operations. 
 */
final class InterceptableUpdateCallback extends AbstractUpdateCallback implements UpdateCallback {

    private final UpdateCallback _updateCallback;
    private final InterceptorList<TableCreationBuilder> _tableCreationInterceptors;
    private final InterceptorList<TableDropBuilder> _tableDropInterceptors;
    private final InterceptorList<RowInsertionBuilder> _rowInsertionInterceptors;
    private final InterceptorList<RowUpdationBuilder> _rowUpdationInterceptors;
    private final InterceptorList<RowDeletionBuilder> _rowDeletionInterceptors;

    public InterceptableUpdateCallback(InterceptableDataContext dataContext, UpdateCallback updateCallback,
            InterceptorList<TableCreationBuilder> tableCreationInterceptors,
            InterceptorList<TableDropBuilder> tableDropInterceptors,
            InterceptorList<RowInsertionBuilder> rowInsertionInterceptors,
            InterceptorList<RowUpdationBuilder> rowUpdationInterceptors,
            InterceptorList<RowDeletionBuilder> rowDeletionInterceptors) {
        super(dataContext);
        _updateCallback = updateCallback;
        _tableCreationInterceptors = tableCreationInterceptors;
        _tableDropInterceptors = tableDropInterceptors;
        _rowInsertionInterceptors = rowInsertionInterceptors;
        _rowUpdationInterceptors = rowUpdationInterceptors;
        _rowDeletionInterceptors = rowDeletionInterceptors;
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        TableCreationBuilder tableCreationBuilder = _updateCallback.createTable(schema, name);
        if (_tableCreationInterceptors.isEmpty()) {
            return tableCreationBuilder;
        }
        return new InterceptableTableCreationBuilder(tableCreationBuilder, _tableCreationInterceptors);
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException {
        RowInsertionBuilder rowInsertionBuilder = _updateCallback.insertInto(table);
        if (_rowInsertionInterceptors.isEmpty()) {
            return rowInsertionBuilder;
        }
        return new InterceptableRowInsertionBuilder(rowInsertionBuilder, _rowInsertionInterceptors);
    }

    @Override
    public boolean isCreateTableSupported() {
        return _updateCallback.isCreateTableSupported();
    }

    @Override
    public boolean isDropTableSupported() {
        return _updateCallback.isDropTableSupported();
    }

    @Override
    public TableDropBuilder dropTable(Table table) {
        TableDropBuilder tableDropBuilder = _updateCallback.dropTable(table);
        if (_tableDropInterceptors.isEmpty()) {
            return tableDropBuilder;
        }
        return new InterceptableTableDropBuilder(tableDropBuilder, _tableDropInterceptors);
    }

    @Override
    public boolean isInsertSupported() {
        return _updateCallback.isInsertSupported();
    }

    @Override
    public boolean isUpdateSupported() {
        return _updateCallback.isUpdateSupported();
    }

    @Override
    public RowUpdationBuilder update(Table table) {
        RowUpdationBuilder rowUpdationBuilder = _updateCallback.update(table);
        if (_rowUpdationInterceptors.isEmpty()) {
            return rowUpdationBuilder;
        }
        return new InterceptableRowUpdationBuilder(rowUpdationBuilder, _rowUpdationInterceptors);
    }

    @Override
    public boolean isDeleteSupported() {
        return _updateCallback.isDeleteSupported();
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) {
        RowDeletionBuilder rowDeletionBuilder = _updateCallback.deleteFrom(table);
        if (_rowDeletionInterceptors.isEmpty()) {
            return rowDeletionBuilder;
        }
        return new InterceptableRowDeletionBuilder(rowDeletionBuilder, _rowDeletionInterceptors);
    }
}
