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

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import com.redshoes.metamodel.AbstractUpdateCallback;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.create.TableCreationBuilder;
import com.redshoes.metamodel.delete.RowDeletionBuilder;
import com.redshoes.metamodel.drop.TableDropBuilder;
import com.redshoes.metamodel.insert.RowInsertionBuilder;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


final class MongoDbUpdateCallback extends AbstractUpdateCallback implements UpdateCallback, Closeable {

    private final MongoDbDataContext _dataContext;
    private final Map<String, MongoCollection<Document>> _collections;
    private final WriteConcernAdvisor _writeConcernAdvisor;

    public MongoDbUpdateCallback(MongoDbDataContext dataContext, WriteConcernAdvisor writeConcernAdvisor) {
        super(dataContext);
        _dataContext = dataContext;
        _writeConcernAdvisor = writeConcernAdvisor;
        _collections = new HashMap<String, MongoCollection<Document>>();
    }

    @Override
    public MongoDbDataContext getDataContext() {
        return _dataContext;
    }
    
    public WriteConcernAdvisor getWriteConcernAdvisor() {
        return _writeConcernAdvisor;
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        return new MongoDbTableCreationBuilder(this, schema, name);
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException {
        return new MongoDbInsertionBuilder(this, table);
    }

    protected void createCollection(String name) {
        MongoDatabase mongoDb = _dataContext.getMongoDb();
        mongoDb.createCollection(name);
        MongoCollection<Document> collection = mongoDb.getCollection(name);
        _collections.put(name, collection);
    }

    protected void removeCollection(String name) {
        MongoCollection<Document> collection = getCollection(name);
        _collections.remove(name);
        collection.drop();
    }

    protected MongoCollection<Document> getCollection(String name) {
        MongoCollection<Document> collection = _collections.get(name);
        if (collection == null) {
            collection = _dataContext.getMongoDb().getCollection(name);
            _collections.put(name, collection);
        }
        return collection;
    }

    @Override
    public void close() {
        _collections.clear();
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws UnsupportedOperationException {
        return new MongoDbDropTableBuilder(this, table);
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new MongoDbDeleteBuilder(this, table);
    }
}
