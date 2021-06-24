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

import java.util.Iterator;
import java.util.List;

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.delete.AbstractRowDeletionBuilder;
import com.redshoes.metamodel.delete.RowDeletionBuilder;
import com.redshoes.metamodel.elasticsearch.common.ElasticSearchUtils;
import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.LogicalOperator;
import com.redshoes.metamodel.schema.Table;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RowDeletionBuilder} implementation for
 * {@link ElasticSearchDataContext}.
 * 
 * @deprecated {@link TransportClient} on which this implementation is based is deprecated in Elasticsearch 7.x and will
 *             be removed in Elasticsearch 8. Please use ElasticSearchRestDeleteBuilder instead.
 */
@Deprecated
final class ElasticSearchDeleteBuilder extends AbstractRowDeletionBuilder {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchDeleteBuilder.class);

    private final ElasticSearchUpdateCallback _updateCallback;

    public ElasticSearchDeleteBuilder(ElasticSearchUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        final Table table = getTable();
        final String documentType = table.getName();

        final ElasticSearchDataContext dataContext = _updateCallback.getDataContext();
        final Client client = dataContext.getElasticSearchClient();
        final String indexName = dataContext.getIndexName();

        final List<FilterItem> whereItems = getWhereItems();

        // delete by query - note that creteQueryBuilderForSimpleWhere may
        // return matchAllQuery() if no where items are present.
        final QueryBuilder queryBuilder = ElasticSearchUtils.createQueryBuilderForSimpleWhere(whereItems,
                LogicalOperator.AND);
        if (queryBuilder == null) {
            // TODO: The where items could not be pushed down to a query. We
            // could solve this by running a query first, gather all
            // document IDs and then delete by IDs.
            throw new UnsupportedOperationException("Could not push down WHERE items to delete by query request: "
                    + whereItems);
        }

        final SearchResponse response =
                client.prepareSearch(indexName).setQuery(queryBuilder).setTypes(documentType).execute()
                        .actionGet();

        client.admin().indices().prepareRefresh(indexName).execute().actionGet();
        final Iterator<SearchHit> iterator = response.getHits().iterator();
        while (iterator.hasNext()) {
            final SearchHit hit = iterator.next();
            final String typeId = hit.getId();
            final DeleteResponse deleteResponse =
                    client.prepareDelete().setIndex(indexName).setType(documentType).setId(typeId).execute()
                            .actionGet();
            logger.debug("Deleted documents by query." + deleteResponse.getResult());
        }
        client.admin().indices().prepareRefresh(indexName).execute().actionGet();
    }
}
