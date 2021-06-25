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
package com.redshoes.metamodel.jdbc.dialects;

import com.redshoes.metamodel.jdbc.JdbcDataContext;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.schema.ColumnSize;
import com.redshoes.metamodel.schema.ColumnType;

/**
 * Query rewriter for Apache Hive
 */
public class HiveQueryRewriter extends RowNumberQueryRewriter {

    public HiveQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String rewriteQuery(Query query) {

        Integer maxRows = query.getMaxRows();
        Integer firstRow = query.getFirstRow();

        if (maxRows == null && (firstRow == null || firstRow.intValue() == 1)) {
            return super.rewriteQuery(query);
        }

        if ((firstRow == null || firstRow.intValue() == 1) && maxRows != null && maxRows > 0) {
            // We prefer to use the "LIMIT n" approach, if
            // firstRow is not specified.
            return super.rewriteQuery(query) + " LIMIT " + maxRows;
        } else {
            return getRowNumberSql(query, maxRows, firstRow);
        }

    }

    @Override
    public String rewriteColumnType(ColumnType columnType, ColumnSize columnSize) {
        if (columnType == ColumnType.INTEGER) {
            return "INT";
        }

        if(columnType == ColumnType.STRING) {
            return "STRING";
        }

        // Hive does not support VARCHAR without a width, nor VARCHAR(MAX).
        // Returning max allowable column size instead.
        if (columnType == ColumnType.VARCHAR && columnSize == null) {
            return super.rewriteColumnType(columnType, ColumnSize.of(65535));
        }
        return super.rewriteColumnType(columnType, columnSize);
    }
    
    @Override
    public boolean isTransactional() {
        return false;
    }
    
	@Override
	public boolean isPrimaryKeySupported() {
		return false;
	}
}
