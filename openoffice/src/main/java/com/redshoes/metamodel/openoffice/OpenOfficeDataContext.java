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
package com.redshoes.metamodel.openoffice;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.redshoes.metamodel.AbstractDataContext;
import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.TableType;
import com.redshoes.metamodel.util.FileHelper;
import com.redshoes.metamodel.jdbc.JdbcDataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenOfficeDataContext extends AbstractDataContext implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(OpenOfficeDataContext.class);

    private final JdbcDataContext _dataContextDelegate;
    private final Connection _connection;

    public OpenOfficeDataContext(File dbFile) throws MetaModelException {
        try {
            String databaseName = dbFile.getName();
            File tempDir = FileHelper.getTempDir();

            ZipFile zipFile = new ZipFile(dbFile);
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                String entryName = entry.getName();
                if (entryName.startsWith("database/")) {
                    File destFile = new File(tempDir, databaseName + "." + entryName.substring(9));
                    destFile.createNewFile();

                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing entry: " + entryName);
                        logger.debug("Writing temp file: " + destFile.getAbsolutePath());
                    }

                    byte[] buffer = new byte[1024];
                    InputStream inputStream = zipFile.getInputStream(entry);
                    OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(destFile));
                    for (int len = inputStream.read(buffer); len >= 0; len = inputStream.read(buffer)) {
                        outputStream.write(buffer, 0, len);
                    }
                    inputStream.close();
                    outputStream.close();
                    destFile.deleteOnExit();
                } else {
                    logger.debug("Ignoring entry: " + entryName);
                }
            }
            zipFile.close();

            Class.forName("org.hsqldb.jdbcDriver");
            String url = "jdbc:hsqldb:file:" + tempDir.getAbsolutePath() + "/" + databaseName;
            logger.info("Using database URL: " + url);
            _connection = DriverManager.getConnection(url, "SA", "");
            _connection.setReadOnly(true);
            _dataContextDelegate = new JdbcDataContext(_connection, TableType.DEFAULT_TABLE_TYPES, null);
        } catch (Exception e) {
            throw new MetaModelException(e);
        }
    }

    @Override
    public DataSet executeQuery(Query query) throws MetaModelException {
        return _dataContextDelegate.executeQuery(query);
    }

    @Override
    public void close() {
        try {
            _connection.close();
        } catch (SQLException e) {
            logger.warn("Failed to close underlying JDBC connection: {}", e.getMessage(), e);
        }
    }

    public Connection getConnection() {
        return _connection;
    }

    @Override
    protected List<String> getSchemaNamesInternal() {
        return _dataContextDelegate.getSchemaNames();
    }

    @Override
    protected String getDefaultSchemaName() {
        return _dataContextDelegate.getDefaultSchemaName();
    }

    @Override
    protected Schema getSchemaByNameInternal(String name) {
        return _dataContextDelegate.getSchemaByName(name);
    }
}