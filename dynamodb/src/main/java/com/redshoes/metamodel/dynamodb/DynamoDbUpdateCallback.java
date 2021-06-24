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
package com.redshoes.metamodel.dynamodb;

import com.redshoes.metamodel.AbstractUpdateCallback;
import com.redshoes.metamodel.create.TableCreationBuilder;
import com.redshoes.metamodel.delete.RowDeletionBuilder;
import com.redshoes.metamodel.drop.TableDropBuilder;
import com.redshoes.metamodel.insert.RowInsertionBuilder;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;

final class DynamoDbUpdateCallback extends AbstractUpdateCallback {
    
    private boolean interrupted = false;

    public DynamoDbUpdateCallback(DynamoDbDataContext dataContext) {
        super(dataContext);
    }
    
    public boolean isInterrupted() {
        return interrupted;
    }
    
    public void setInterrupted(boolean interrupted) {
        this.interrupted = interrupted;
    }

    @Override
    public DynamoDbDataContext getDataContext() {
        return (DynamoDbDataContext) super.getDataContext();
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        return new DynamoDbTableCreationBuilder(this, schema, name);
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new DynamoDbTableDropBuilder(table, getDataContext());
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new DynamoDbRowInsertionBuilder(this, table);
    }

    @Override
    public boolean isDeleteSupported() {
        return false;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        // This could be implemented ...
        throw new UnsupportedOperationException();
    }

}
