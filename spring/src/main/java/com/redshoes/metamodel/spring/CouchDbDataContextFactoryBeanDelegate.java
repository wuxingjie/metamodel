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
package com.redshoes.metamodel.spring;

import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.DataContextFactory;
import com.redshoes.metamodel.couchdb.CouchDbDataContext;
import com.redshoes.metamodel.util.SimpleTableDef;

/**
 * {@link DataContextFactoryBeanDelegate} for {@link CouchDbDataContext}.
 */
public class CouchDbDataContextFactoryBeanDelegate extends AbstractDataContextFactoryBeanDelegate {

    @Override
    public DataContext createDataContext(DataContextFactoryParameters params) {
        String hostname = params.getHostname();
        Integer port = params.getPort();
        String username = params.getUsername();
        String password = params.getPassword();
        SimpleTableDef[] tableDefs = params.getTableDefs();
        return DataContextFactory.createCouchDbDataContext(hostname, port, username, password, tableDefs);
    }

}
