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
package com.redshoes.metamodel.cassandra;

import com.datastax.driver.core.Cluster;

/**
 * Utility test class that provides a handy way to
 * connect to a Cassandra cluster through the
 * {@link Cluster} class.
 *
 * To get a connected instance of the cluster you
 * should call {@link #connect(String, int)}
 * providing the node and port of your Cassandra cluster
 * then you can get the connected instance by calling
 * {@link #getCluster()}.
 *
 */
public class CassandraSimpleClient {
    private Cluster cluster;

    public void connect(String node, int port) {
        cluster = Cluster.builder().withPort(port)
                .addContactPoint(node)
                .build();
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void close() {
        if (cluster != null) {            
            cluster.close();
        }
    }
}
