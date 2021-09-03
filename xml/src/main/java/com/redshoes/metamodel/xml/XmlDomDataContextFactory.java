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
package com.redshoes.metamodel.xml;

import com.redshoes.metamodel.ConnectionException;
import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.factory.DataContextFactory;
import com.redshoes.metamodel.factory.DataContextProperties;
import com.redshoes.metamodel.factory.ResourceFactoryRegistry;
import com.redshoes.metamodel.factory.UnsupportedDataContextPropertiesException;
import com.redshoes.metamodel.util.BooleanComparator;
import com.redshoes.metamodel.util.Resource;

public class XmlDomDataContextFactory implements DataContextFactory {

    @Override
    public boolean accepts(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry) {
        return "xml".equals(properties.getDataContextType()) && !properties.toMap().containsKey("table-defs");
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {
        final Resource resource = resourceFactoryRegistry.createResource(properties.getResourceProperties());

        boolean autoFlattenTables = true;
        if (properties.toMap().get("auto-flatten-tables") != null) {
            autoFlattenTables = BooleanComparator.toBoolean(properties.toMap().get("auto-flatten-tables"));
        }

        return new XmlDomDataContext(resource, autoFlattenTables);
    }
}
