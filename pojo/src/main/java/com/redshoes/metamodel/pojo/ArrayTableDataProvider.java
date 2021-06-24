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
package com.redshoes.metamodel.pojo;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.redshoes.metamodel.util.SimpleTableDef;

/**
 * {@link TableDataProvider} based on an {@link Collection} (for instance a
 * {@link List}) of object arrays.
 */
public class ArrayTableDataProvider implements TableDataProvider<Object[]> {

    private static final long serialVersionUID = 1L;
    private final SimpleTableDef _tableDef;
    private final Collection<Object[]> _arrays;

    public ArrayTableDataProvider(SimpleTableDef tableDef, Collection<Object[]> arrays) {
        _tableDef = tableDef;
        _arrays = arrays;
    }

    @Override
    public String getName() {
        return getTableDef().getName();
    }

    @Override
    public Iterator<Object[]> iterator() {
        return _arrays.iterator();
    }

    @Override
    public SimpleTableDef getTableDef() {
        return _tableDef;
    }

    @Override
    public Object getValue(String columnName, Object[] record) {
        int index = _tableDef.indexOf(columnName);
        return record[index];
    }

    @Override
    public void insert(Map<String, Object> recordData) {
        String[] columnNames = _tableDef.getColumnNames();
        Object[] record = new Object[columnNames.length];
        for (int i = 0; i < record.length; i++) {
            record[i] = recordData.get(columnNames[i]);
        }
        _arrays.add(record);
    }

}
