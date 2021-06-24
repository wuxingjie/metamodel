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
package com.redshoes.metamodel.mongodb.mongo2;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.UpdateScript;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.data.Row;
import com.redshoes.metamodel.insert.RowInsertionBuilder;
import com.redshoes.metamodel.jdbc.JdbcDataContext;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.util.FileHelper;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * Simple example program that can copy data to a MongoDB collection
 */
public class MongoDbDataCopyer {

    private final DataContext _sourceDataContext;
    private final DB _mongoDb;
    private final String _collectionName;
    private final String _sourceSchemaName;
    private final String _sourceTableName;

    // example copy job that will populate the mongodb with Derby data
    public static void main(String[] args) throws Exception {
        System.setProperty("derby.storage.tempDirector", FileHelper.getTempDir().getAbsolutePath());
        System.setProperty("derby.stream.error.file", File.createTempFile("metamodel-derby", ".log").getAbsolutePath());

        File dbFile = new File("../jdbc/src/test/resources/derby_testdb.jar");
        dbFile = dbFile.getCanonicalFile();
        if (!dbFile.exists()) {
            throw new IllegalStateException("File does not exist: " + dbFile);
        }

        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection connection = DriverManager.getConnection("jdbc:derby:jar:(" + dbFile.getAbsolutePath()
                + ")derby_testdb;territory=en");
        connection.setReadOnly(true);

        DB db = new Mongo().getDB("orderdb_copy");

        DataContext sourceDataContext = new JdbcDataContext(connection);

        new MongoDbDataCopyer(db, "orders", sourceDataContext, "APP", "orders").copy();
        new MongoDbDataCopyer(db, "offices", sourceDataContext, "APP", "offices").copy();
        new MongoDbDataCopyer(db, "payments", sourceDataContext, "APP", "payments").copy();
        new MongoDbDataCopyer(db, "orderfact", sourceDataContext, "APP", "orderfact").copy();
        new MongoDbDataCopyer(db, "products", sourceDataContext, "APP", "products").copy();

        connection.close();
    }

    public MongoDbDataCopyer(DB mongoDb, String collectionName, DataContext sourceDataContext, String sourceSchemaName,
            String sourceTableName) {
        _mongoDb = mongoDb;
        _collectionName = collectionName;
        _sourceDataContext = sourceDataContext;
        _sourceSchemaName = sourceSchemaName;
        _sourceTableName = sourceTableName;
    }

    public void copy() {
        final MongoDbDataContext targetDataContext = new MongoDbDataContext(_mongoDb);
        targetDataContext.executeUpdate(new UpdateScript() {

            @Override
            public void run(UpdateCallback callback) {
                final Table sourceTable = getSourceTable();
                final Table targetTable = callback.createTable(targetDataContext.getDefaultSchema(), _collectionName)
                        .like(sourceTable).execute();
                final List<Column> sourceColumns = sourceTable.getColumns();
                final DataSet dataSet = _sourceDataContext.query().from(sourceTable).select(sourceColumns).execute();
                while (dataSet.next()) {
                    final Row row = dataSet.getRow();

                    RowInsertionBuilder insertBuilder = callback.insertInto(targetTable);
                    for (Column column : sourceColumns) {
                        insertBuilder = insertBuilder.value(column.getName(), row.getValue(column));
                    }
                    insertBuilder.execute();
                }
                dataSet.close();
            }
        });
    }

    private Table getSourceTable() {
        final Schema schema;
        if (_sourceSchemaName != null) {
            schema = _sourceDataContext.getSchemaByName(_sourceSchemaName);
        } else {
            schema = _sourceDataContext.getDefaultSchema();
        }

        return schema.getTableByName(_sourceTableName);
    }
}
