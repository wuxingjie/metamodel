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
package com.redshoes.metamodel.couchdb;

import com.redshoes.metamodel.convert.DocumentConverter;
import com.redshoes.metamodel.data.Document;
import com.redshoes.metamodel.data.DocumentSource;
import com.redshoes.metamodel.schema.MutableTable;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.schema.builder.DocumentSourceProvider;
import com.redshoes.metamodel.schema.builder.InferentialSchemaBuilder;
import com.redshoes.metamodel.schema.builder.InferentialTableBuilder;

final class CouchDbInferentialSchemaBuilder extends InferentialSchemaBuilder {
    
    private final String[] _databaseNames;

    public CouchDbInferentialSchemaBuilder() {
        this(null);
    }

    public CouchDbInferentialSchemaBuilder(String[] databaseNames) {
        super(CouchDbDataContext.SCHEMA_NAME);
        _databaseNames = databaseNames;
    }
    
    @Override
    public void offerSources(DocumentSourceProvider documentSourceProvider) {
        if (_databaseNames == null) {
            super.offerSources(documentSourceProvider);
        } else {
            for (String databaseName : _databaseNames) {
                final DocumentSource documentSource = documentSourceProvider.getDocumentSourceForTable(databaseName);
                offerDocumentSource(documentSource);
            }
        }
    }

    @Override
    public DocumentConverter getDocumentConverter(Table table) {
        return new CouchDbDocumentConverter();
    }
    
    @Override
    protected String determineTable(Document document) {
        final String sourceCollectionName = document.getSourceCollectionName();
        return sourceCollectionName;
    }
    
    @Override
    protected MutableTable buildTable(InferentialTableBuilder tableBuilder) {
        final MutableTable table = super.buildTable(tableBuilder);
        CouchDbTableCreationBuilder.addMandatoryColumns(table);
        return table;
    }
}
