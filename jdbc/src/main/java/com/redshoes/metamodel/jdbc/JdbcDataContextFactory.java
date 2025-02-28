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
package com.redshoes.metamodel.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.redshoes.metamodel.ConnectionException;
import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.factory.AbstractDataContextFactory;
import com.redshoes.metamodel.factory.DataContextProperties;
import com.redshoes.metamodel.factory.ResourceFactoryRegistry;
import com.redshoes.metamodel.schema.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDataContextFactory extends AbstractDataContextFactory {

    private static final Logger logger = LoggerFactory.getLogger(JdbcDataContextFactory.class);

    @Override
    protected String getType() {
        return "jdbc";
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws ConnectionException {
        final String driverClassName = properties.getDriverClassName();
        if (driverClassName != null) {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                logger.warn("Failed to initialize driver class: {}", driverClassName, e);
            }
        }

        final TableType[] tableTypes =
                properties.getTableTypes() == null ? TableType.DEFAULT_TABLE_TYPES : properties.getTableTypes();
        final String catalogName = properties.getCatalogName();

        final DataSource dataSource = properties.getDataSource();
        if (dataSource != null) {
            return new JdbcDataContext(dataSource, tableTypes, catalogName);
        }

        final String url = properties.getUrl();
        final String username = properties.getUsername();
        final String password = properties.getPassword();

        final Connection connection;
        try {
            if (username != null) {
                connection = DriverManager.getConnection(url, username, password);
            } else {
                connection = DriverManager.getConnection(url);
            }
        } catch (SQLException e) {
            throw new ConnectionException("Failed to open JDBC connection from URL: " + url, e);
        }

        return new JdbcDataContext(connection, tableTypes, catalogName);
    }

}
