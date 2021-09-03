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
package com.redshoes.metamodel.json;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.QueryPostprocessDataContext;
import com.redshoes.metamodel.convert.DocumentConverter;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.schema.builder.DocumentSourceProvider;
import com.redshoes.metamodel.schema.builder.SchemaBuilder;
import com.redshoes.metamodel.schema.builder.SingleTableInferentialSchemaBuilder;
import com.redshoes.metamodel.util.FileHelper;
import com.redshoes.metamodel.util.FileResource;
import com.redshoes.metamodel.util.Resource;
import com.redshoes.metamodel.data.CachingDataSetHeader;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.data.DataSetHeader;
import com.redshoes.metamodel.data.DocumentSource;
import com.redshoes.metamodel.data.DocumentSourceDataSet;
import com.redshoes.metamodel.data.MaxRowsDataSet;
import com.redshoes.metamodel.data.MaxRowsDocumentSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingJsonFactory;

/**
 * {@link DataContext} implementation that works on JSON files or
 * {@link Resource}s.
 */
public class JsonDataContext extends QueryPostprocessDataContext implements DocumentSourceProvider {

    private static final Logger logger = LoggerFactory.getLogger(JsonDataContext.class);

    private final Resource _resource;
    private final SchemaBuilder _schemaBuilder;

    public JsonDataContext(File file) {
        this(new FileResource(file));
    }

    public JsonDataContext(Resource resource) {
        this(resource, new SingleTableInferentialSchemaBuilder(resource));
    }

    public JsonDataContext(Resource resource, SchemaBuilder schemaBuilder) {
        super(false);
        _resource = resource;
        _schemaBuilder = schemaBuilder;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        _schemaBuilder.offerSources(this);
        return _schemaBuilder.build();
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _schemaBuilder.getSchemaName();
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        final DocumentConverter documentConverter = _schemaBuilder.getDocumentConverter(table);
        final List<SelectItem> selectItems = columns.stream().map(SelectItem::new).collect(Collectors.toList());
        final DataSetHeader header = new CachingDataSetHeader(selectItems);
        final DocumentSource documentSource = getDocumentSourceForTable(table.getName());

        DataSet dataSet = new DocumentSourceDataSet(header, documentSource, documentConverter);

        if (maxRows > 0) {
            dataSet = new MaxRowsDataSet(dataSet, maxRows);
        }

        return dataSet;
    }

    private DocumentSource createDocumentSource() {
        final InputStream inputStream = _resource.read();
        try {
            final MappingJsonFactory jsonFactory = new MappingJsonFactory();
            final JsonParser parser = jsonFactory.createParser(inputStream);
            logger.debug("Created JSON parser for resource: {}", _resource);

            return new JsonDocumentSource(parser, _resource.getName());
        } catch (Exception e) {
            FileHelper.safeClose(inputStream);
            throw new MetaModelException("Unexpected error while creating JSON parser", e);
        }
    }

    @Override
    public DocumentSource getMixedDocumentSourceForSampling() {
        return new MaxRowsDocumentSource(createDocumentSource(), 1000);
    }

    @Override
    public DocumentSource getDocumentSourceForTable(String sourceCollectionName) {
        // only a single source collection - returning that
        return createDocumentSource();
    }
}
