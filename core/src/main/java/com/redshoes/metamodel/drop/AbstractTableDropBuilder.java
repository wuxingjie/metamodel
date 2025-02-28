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
package com.redshoes.metamodel.drop;

import com.redshoes.metamodel.schema.Table;

/**
 * Abstract {@link TableDropBuilder} implementation
 */
public abstract class AbstractTableDropBuilder implements TableDropBuilder {

    private final Table _table;

    public AbstractTableDropBuilder(Table table) {
        if (table == null) {
            throw new IllegalArgumentException("Table cannot be null");
        }
        _table = table;
    }

    @Override
    public final Table getTable() {
        return _table;
    }
    
    @Override
    public String toString() {
        return toSql();
    }
    
    @Override
    public String toSql() {
        return "DROP TABLE " + _table.getQualifiedLabel();
    }
}
