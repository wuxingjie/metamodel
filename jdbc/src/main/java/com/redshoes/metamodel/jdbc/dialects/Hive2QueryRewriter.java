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
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.jdbc.JdbcDataContext;

/**
 * Query rewriter for Apache Hive
 */
public class Hive2QueryRewriter extends LimitOffsetQueryRewriter {

    public Hive2QueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String rewriteColumnType(ColumnType columnType, Integer columnSize,Integer decimalDigits) {
        if (columnType == ColumnType.INTEGER) {
            return "INT";
        }

        if (columnType == ColumnType.STRING) {
            return "STRING";
        }

        // Hive does not support VARCHAR without a width, nor VARCHAR(MAX).
        // Returning max allowable column size instead.
        if (columnType == ColumnType.VARCHAR && columnSize == null) {
            return super.rewriteColumnType(columnType, 65535, null);
        }
        return super.rewriteColumnType(columnType, columnSize, decimalDigits);
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
