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
package com.redshoes.metamodel.dynamodb;

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.drop.AbstractTableDropBuilder;
import com.redshoes.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;

final class DynamoDbTableDropBuilder extends AbstractTableDropBuilder {

    private static final Logger logger = LoggerFactory.getLogger(DynamoDbTableDropBuilder.class);
    private final DynamoDbDataContext _dataContext;

    public DynamoDbTableDropBuilder(Table table, DynamoDbDataContext dataContext) {
        super(table);
        _dataContext = dataContext;
    }

    @Override
    public void execute() throws MetaModelException {
        final String tableName = getTable().getName();
        final DeleteTableResult result = _dataContext.getDynamoDb().deleteTable(tableName);
        logger.debug("Dropped table {} in request ID: {}", tableName, result.getSdkResponseMetadata().getRequestId());
    }

}
