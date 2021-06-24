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
package com.redshoes.metamodel.elasticsearch.nativeclient;

import com.redshoes.metamodel.AbstractUpdateCallback;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.create.TableCreationBuilder;
import com.redshoes.metamodel.delete.RowDeletionBuilder;
import com.redshoes.metamodel.drop.TableDropBuilder;
import com.redshoes.metamodel.insert.RowInsertionBuilder;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.update.RowUpdationBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;

/**
 * {@link UpdateCallback} implementation for {@link ElasticSearchDataContext}.
 * 
 * @deprecated {@link TransportClient} on which this implementation is based is deprecated in Elasticsearch 7.x and will
 *             be removed in Elasticsearch 8. Please use ElasticSearchUpdateCallback instead.
 */
@Deprecated
final class ElasticSearchUpdateCallback extends AbstractUpdateCallback {

    public ElasticSearchUpdateCallback(ElasticSearchDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public ElasticSearchDataContext getDataContext() {
        return (ElasticSearchDataContext) super.getDataContext();
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        return new ElasticSearchCreateTableBuilder(this, schema, name);
    }

    @Override
    public boolean isDropTableSupported() {
        return false;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new ElasticSearchInsertBuilder(this, table);
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new ElasticSearchDeleteBuilder(this, table);
    }

    @Override
    public RowUpdationBuilder update(final Table table) {
        return new ElasticSearchUpdateBuilder(this, table);
    }

    public void onExecuteUpdateFinished() {
        // force refresh of the index
        final ElasticSearchDataContext dataContext = getDataContext();
        final Client client = dataContext.getElasticSearchClient();
        final String indexName = dataContext.getIndexName();
        client.admin().indices().prepareRefresh(indexName).execute().actionGet();
    }

}
