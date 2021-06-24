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
package com.redshoes.metamodel.fixedwidth;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.List;

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.QueryPostprocessDataContext;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.schema.naming.ColumnNamingContextImpl;
import com.redshoes.metamodel.schema.naming.ColumnNamingSession;
import com.redshoes.metamodel.schema.naming.ColumnNamingStrategy;
import com.redshoes.metamodel.util.FileHelper;
import com.redshoes.metamodel.util.FileResource;
import com.redshoes.metamodel.util.Resource;
import com.redshoes.metamodel.util.ResourceUtils;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.MutableColumn;
import com.redshoes.metamodel.schema.MutableSchema;
import com.redshoes.metamodel.schema.MutableTable;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.schema.TableType;

/**
 * DataContext implementation for fixed width value files.
 */
public class FixedWidthDataContext extends QueryPostprocessDataContext {

    private final Resource _resource;
    private final FixedWidthConfiguration _configuration;

    public FixedWidthDataContext(File file, FixedWidthConfiguration configuration) {
        super(true);
        _resource = new FileResource(file);
        _configuration = configuration;
    }

    public FixedWidthDataContext(Resource resource, FixedWidthConfiguration configuration) {
        super(true);
        _resource = resource;
        _configuration = configuration;
    }

    /**
     * Gets the Fixed width value configuration used.
     * 
     * @return a fixed width configuration
     */
    public FixedWidthConfiguration getConfiguration() {
        return _configuration;
    }

    /**
     * Gets the resource being read
     * 
     * @return a {@link Resource} object
     */
    public Resource getResource() {
        return _resource;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final String schemaName = getDefaultSchemaName();
        final MutableSchema schema = new MutableSchema(schemaName);
        final String tableName = _resource.getName();
        final MutableTable table = new MutableTable(tableName, TableType.TABLE, schema);
        schema.addTable(table);

        final FixedWidthReader reader = createReader();
        final String[] columnNames;
        try {
            final boolean hasColumnHeader = _configuration
                    .getColumnNameLineNumber() != FixedWidthConfiguration.NO_COLUMN_NAME_LINE;
            if (hasColumnHeader) {
                for (int i = 1; i < _configuration.getColumnNameLineNumber(); i++) {
                    reader.readLine();
                }
                columnNames = reader.readLine();
            } else {
                columnNames = reader.readLine();
            }
            final ColumnNamingStrategy columnNamingStrategy = _configuration.getColumnNamingStrategy();
            if (columnNames != null) {
                try (final ColumnNamingSession columnNamingSession = columnNamingStrategy.startColumnNamingSession()) {
                    for (int i = 0; i < columnNames.length; i++) {
                        final String intrinsicColumnName = hasColumnHeader ? columnNames[i] : null;
                        columnNames[i] = columnNamingSession.getNextColumnName(new ColumnNamingContextImpl(table,
                                intrinsicColumnName, i));
                    }
                }
            }
        } finally {
            FileHelper.safeClose(reader);
        }

        if (columnNames != null) {
            for (int i = 0; i < columnNames.length; i++) {
                final String columnName = columnNames[i];
                final MutableColumn column = new MutableColumn(columnName, ColumnType.STRING, table, i, true);
                column.setColumnSize(_configuration.getValueWidth(i));
                table.addColumn(column);
            }
        }

        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return ResourceUtils.getParentName(_resource);
    }

    @Override
    public DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        final FixedWidthReader reader = createReader();
        try {
            for (int i = 1; i <= _configuration.getColumnNameLineNumber(); i++) {
                reader.readLine();
            }
        } catch (IllegalStateException e) {
            FileHelper.safeClose(reader);
            throw e;
        }
        if (maxRows > 0) {
            return new FixedWidthDataSet(reader, columns, maxRows);
        } else {
            return new FixedWidthDataSet(reader, columns, null);
        }
    }

    private FixedWidthReader createReader() {
        final InputStream inputStream = _resource.read();
        final FixedWidthReader reader;

        if (_configuration instanceof EbcdicConfiguration) {
            final BufferedInputStream bufferedInputStream =
                    inputStream instanceof BufferedInputStream ? (BufferedInputStream) inputStream
                            : new BufferedInputStream(inputStream);
            reader =
                    new EbcdicReader(bufferedInputStream, _configuration.getEncoding(), _configuration.getValueWidths(),
                            _configuration.isFailOnInconsistentLineWidth(),
                            ((EbcdicConfiguration) _configuration).isSkipEbcdicHeader(),
                            ((EbcdicConfiguration) _configuration).isEolPresent());
        } else {
            if (_configuration.isConstantValueWidth()) {
                reader = new FixedWidthReader(inputStream, _configuration.getEncoding(),
                        _configuration.getFixedValueWidth(), _configuration.isFailOnInconsistentLineWidth());
            } else {
                reader = new FixedWidthReader(inputStream, _configuration.getEncoding(),
                        _configuration.getValueWidths(), _configuration.isFailOnInconsistentLineWidth());
            }
        }

        return reader;
    }
}
