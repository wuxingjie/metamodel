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

import com.redshoes.metamodel.AbstractUpdateCallback;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.create.TableCreationBuilder;
import com.redshoes.metamodel.delete.RowDeletionBuilder;
import com.redshoes.metamodel.drop.TableDropBuilder;
import com.redshoes.metamodel.insert.RowInsertionBuilder;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;

/**
 * This class is used to build objects to do client-operations on a HBase datastore
 */
final class HBaseUpdateCallback extends AbstractUpdateCallback implements UpdateCallback {

    public HBaseUpdateCallback(final HBaseDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public TableCreationBuilder createTable(final Schema schema, final String name) {
        return new HBaseCreateTableBuilder(this, schema, name);
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(final Table table) {
        return new HBaseTableDropBuilder(table, this);
    }

    @Override
    public RowInsertionBuilder insertInto(final Table table) {
        if (table instanceof HBaseTable) {
            return new HBaseRowInsertionBuilder(this, (HBaseTable) table);
        } else {
            throw new IllegalArgumentException("Not an HBase table: " + table);
        }
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    /**
     * @throws IllegalArgumentException when table isn't a {@link HBaseTable}
     */
    @Override
    public RowDeletionBuilder deleteFrom(final Table table) {
        if (table instanceof HBaseTable) {
            return new HBaseRowDeletionBuilder(((HBaseDataContext) getDataContext()), table);
        } else {
            throw new IllegalArgumentException("Not an HBase table: " + table);
        }
    }
}
