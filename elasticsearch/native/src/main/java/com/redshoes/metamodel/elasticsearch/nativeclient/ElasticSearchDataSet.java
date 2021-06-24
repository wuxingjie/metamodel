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

import java.util.List;

import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.elasticsearch.AbstractElasticSearchDataSet;
import com.redshoes.metamodel.query.SelectItem;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;

/**
 * {@link DataSet} implementation for ElasticSearch
 * 
 * @deprecated {@link TransportClient} on which this implementation is based is deprecated in Elasticsearch 7.x and will
 *             be removed in Elasticsearch 8. Please use ElasticSearchRestDataSet instead.
 */
@Deprecated
final class ElasticSearchDataSet extends AbstractElasticSearchDataSet {

    private final Client _client;

    public ElasticSearchDataSet(final Client client, final SearchResponse searchResponse,
            final List<SelectItem> selectItems) {
        super(searchResponse, selectItems);
        _client = client;
    }

    @Override
    public void closeNow() {
        ClearScrollRequestBuilder scrollRequestBuilder = new ClearScrollRequestBuilder(_client,
                ClearScrollAction.INSTANCE).addScrollId(_searchResponse.getScrollId());
        scrollRequestBuilder.execute();
    }

    @Override
    protected SearchResponse scrollSearchResponse(final String scrollId) {
        return _client.prepareSearchScroll(scrollId).setScroll(ElasticSearchDataContext.TIMEOUT_SCROLL)
                .execute().actionGet();
    }
}
