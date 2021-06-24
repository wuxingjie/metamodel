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

import java.util.Map;

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.create.AbstractTableCreationBuilder;
import com.redshoes.metamodel.elasticsearch.common.ElasticSearchUtils;
import com.redshoes.metamodel.schema.MutableSchema;
import com.redshoes.metamodel.schema.MutableTable;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated {@link TransportClient} on which this implementation is based is deprecated in Elasticsearch 7.x and will
 *             be removed in Elasticsearch 8. Please use ElasticSearchRestCreateTableBuilder instead.
 */
@Deprecated
final class ElasticSearchCreateTableBuilder extends AbstractTableCreationBuilder<ElasticSearchUpdateCallback> {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchCreateTableBuilder.class);

    public ElasticSearchCreateTableBuilder(ElasticSearchUpdateCallback updateCallback, Schema schema, String name) {
        super(updateCallback, schema, name);
    }

    @Override
    public Table execute() throws MetaModelException {
        final MutableTable table = getTable();
        final Map<String, ?> source = ElasticSearchUtils.getMappingSource(table);

        final ElasticSearchDataContext dataContext = getUpdateCallback().getDataContext();
        final IndicesAdminClient indicesAdmin = dataContext.getElasticSearchClient().admin().indices();
        final String indexName = dataContext.getIndexName();

        final PutMappingRequestBuilder requestBuilder =
                new PutMappingRequestBuilder(indicesAdmin, PutMappingAction.INSTANCE).setIndices(indexName)
                        .setType(table.getName());
        requestBuilder.setSource(source);
        final AcknowledgedResponse result = requestBuilder.execute().actionGet();

        logger.debug("PutMapping response: acknowledged={}", result.isAcknowledged());

        dataContext.getElasticSearchClient().admin().indices().prepareRefresh(indexName).get();

        final MutableSchema schema = (MutableSchema) getSchema();
        schema.addTable(table);
        return table;
    }

}
