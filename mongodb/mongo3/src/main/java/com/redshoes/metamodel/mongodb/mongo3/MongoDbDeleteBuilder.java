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
package com.redshoes.metamodel.mongodb.mongo3;

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.delete.AbstractRowDeletionBuilder;
import com.redshoes.metamodel.schema.Table;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;

final class MongoDbDeleteBuilder extends AbstractRowDeletionBuilder {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbDeleteBuilder.class);

    private final MongoDbUpdateCallback _updateCallback;

    public MongoDbDeleteBuilder(MongoDbUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        final MongoCollection<Document> collection = _updateCallback.getCollection(getTable().getName());

        final MongoDbDataContext dataContext = _updateCallback.getDataContext();
        final Document query = dataContext.createMongoDbQuery(getTable(), getWhereItems(), whereItem -> {
            throw new UnsupportedOperationException("Unable to handle WHERE item in DELETE: " + whereItem.toSql());
        });

        DeleteResult result = collection.deleteMany(query);
        logger.info("Remove returned result: {}", result);
    }

}
