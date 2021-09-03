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

import java.util.HashMap;
import java.util.Map;

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.elasticsearch.common.ElasticSearchUtils;
import com.redshoes.metamodel.insert.AbstractRowInsertionBuilder;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.Table;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated {@link TransportClient} on which this implementation is based is deprecated in Elasticsearch 7.x and will
 *             be removed in Elasticsearch 8. Please use ElasticSearchRestInsertBuilder instead.
 */
@Deprecated
final class ElasticSearchInsertBuilder extends AbstractRowInsertionBuilder<ElasticSearchUpdateCallback> {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchInsertBuilder.class);

    public ElasticSearchInsertBuilder(ElasticSearchUpdateCallback updateCallback, Table table) {
        super(updateCallback, table);
    }

    @Override
    public void execute() throws MetaModelException {
        final ElasticSearchDataContext dataContext = getUpdateCallback().getDataContext();
        final Client client = dataContext.getElasticSearchClient();
        final String indexName = dataContext.getIndexName();
        final String documentType = getTable().getName();
        final IndexRequestBuilder requestBuilder =
                new IndexRequestBuilder(client, IndexAction.INSTANCE).setIndex(indexName).setType(documentType);

        final Map<String, Object> valueMap = new HashMap<>();
        final Column[] columns = getColumns();
        final Object[] values = getValues();
        for (int i = 0; i < columns.length; i++) {
            if (isSet(columns[i])) {
                final String name = columns[i].getName();
                final Object value = values[i];
                if (ElasticSearchUtils.FIELD_ID.equals(name)) {
                    if (value != null) {
                        requestBuilder.setId(value.toString());
                    }
                } else {
                    valueMap.put(name, value);
                }
            }
        }

        assert !valueMap.isEmpty();

        requestBuilder.setSource(valueMap);

        final IndexResponse result = requestBuilder.execute().actionGet();
        
        logger.debug("Inserted document: id={}", result.getId());

        client.admin().indices().prepareRefresh(indexName).execute().actionGet();
    }
}
