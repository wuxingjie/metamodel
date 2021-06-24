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

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.drop.AbstractTableDropBuilder;
import com.redshoes.metamodel.schema.MutableSchema;
import com.redshoes.metamodel.schema.Table;

/**
 * A builder-class to drop tables in a HBase datastore
 */
class HBaseTableDropBuilder extends AbstractTableDropBuilder {
    private final HBaseUpdateCallback _updateCallback;

    public HBaseTableDropBuilder(final Table table, final HBaseUpdateCallback updateCallback) {
        super(table);
        if (updateCallback.getDataContext().getDefaultSchema().getTableByName(table.getName()) == null) {
            throw new MetaModelException("Trying to delete a table that doesn't exist in the datastore.");
        }
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() {
        // Remove from the datastore
        final Table table = getTable();
        ((HBaseDataContext) _updateCallback.getDataContext()).getHBaseClient().dropTable(table.getName());

        // Remove from schema
        ((MutableSchema) table.getSchema()).removeTable(table);
    }
}
