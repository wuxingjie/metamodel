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
package com.redshoes.metamodel.salesforce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import com.redshoes.metamodel.schema.AbstractSchema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.util.LazyRef;

import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

/**
 * Schema implementation for Salesforce, which lazy loads tables based on the
 * "describe" web services.
 */
final class SalesforceSchema extends AbstractSchema {

    private static final long serialVersionUID = 1L;

    private final String _name;
    private final transient Supplier<List<Table>> _tableRef;
    private final transient PartnerConnection _connection;

    public SalesforceSchema(String name, PartnerConnection connection) {
        _name = name;
        _connection = connection;
        _tableRef = new LazyRef<List<Table>>() {
            @Override
            protected List<Table> fetch() {
                final List<Table> result = new ArrayList<Table>();
                final DescribeGlobalResult describeGlobal;
                try {
                    describeGlobal = _connection.describeGlobal();
                } catch (ConnectionException e) {
                    throw SalesforceUtils.wrapException(e, "Failed to invoke describeGlobal service");
                }

                for (final DescribeGlobalSObjectResult sobject : describeGlobal.getSobjects()) {
                    if (sobject.isQueryable() && sobject.isUpdateable()) {
                        final String tableName = sobject.getName();
                        final String tableLabel = sobject.getLabel();

                        final Table table = new SalesforceTable(tableName, tableLabel, SalesforceSchema.this,
                                _connection);
                        result.add(table);
                    }
                }
                return result;
            }
        };
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public List<Table> getTables() {
        return Collections.unmodifiableList(_tableRef.get());
    }

    @Override
    public String getQuote() {
        return null;
    }

}
