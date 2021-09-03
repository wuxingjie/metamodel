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
package com.redshoes.metamodel.elasticsearch.rest;

import java.net.MalformedURLException;
import java.net.URL;

import com.redshoes.metamodel.ConnectionException;
import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.factory.DataContextFactory;
import com.redshoes.metamodel.factory.DataContextProperties;
import com.redshoes.metamodel.factory.ResourceFactoryRegistry;
import com.redshoes.metamodel.factory.UnsupportedDataContextPropertiesException;
import com.redshoes.metamodel.util.SimpleTableDef;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * Factory for ElasticSearch data context of REST type.
 * 
 * The factory will activate when DataContext type is specified as
 * "elasticsearch", "es-rest" or "elasticsearch-rest".
 * 
 * This factory is configured with the following properties:
 * 
 * <ul>
 * <li>url (http or https based base URL of elasticsearch)</li>
 * <li>database (index name)</li>
 * <li>username (optional)</li>
 * <li>password (optional)</li>
 * </ul>
 */
public class ElasticSearchRestDataContextFactory implements DataContextFactory {

    @Override
    public boolean accepts(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry) {
        switch (properties.getDataContextType()) {
        case "elasticsearch":
            // ensure that the url is http or https based to infer that this is
            // a REST based connection
            final String url = properties.getUrl();
            return url != null && url.startsWith("http") && acceptsInternal(properties);
        case "es-rest":
        case "elasticsearch-rest":
            return acceptsInternal(properties);
        }
        return false;
    }

    private boolean acceptsInternal(DataContextProperties properties) {
        if (properties.getUrl() == null) {
            return false;
        }
        if (getIndex(properties) == null) {
            return false;
        }
        return true;
    }

    private RestHighLevelClient createClient(final DataContextProperties properties) throws MalformedURLException {
        final URL url = new URL(properties.getUrl());
        
        return ElasticSearchRestUtil
                .createClient(new HttpHost(url.getHost(), url.getPort()), properties.getUsername(), properties
                        .getPassword());
    }

    private String getIndex(DataContextProperties properties) {
        final String databaseName = properties.getDatabaseName();
        if (databaseName == null) {
            properties.toMap().get("index");
        }
        return databaseName;
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {
        try {
            final RestHighLevelClient client = createClient(properties);
            final String indexName = getIndex(properties);
            final SimpleTableDef[] tableDefinitions = properties.getTableDefs();
            return new ElasticSearchRestDataContext(client, indexName, tableDefinitions);
        } catch (MalformedURLException e) {
            throw new UnsupportedDataContextPropertiesException(e);
        }
    }

}
