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
import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.schema.ColumnSize;
import com.redshoes.metamodel.schema.ColumnType;

/**
 * Query rewriter for Oracle
 */
public class OracleQueryRewriter extends OffsetFetchQueryRewriter {

    public static final int FIRST_FETCH_SUPPORTING_VERSION = 12;

    public OracleQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext, FIRST_FETCH_SUPPORTING_VERSION, false);
    }

    @Override
    public ColumnType getColumnType(int jdbcType, String nativeType, Integer columnSize) {
        // For TIMESTAMP WITH LOCAL_TIME_ZONE/TIME_ZONE, which jdbcType is -102/-101
        if (nativeType.contains("TIMESTAMP")) {
            return ColumnType.TIMESTAMP;
        }
        return super.getColumnType(jdbcType, nativeType, columnSize);
    }

    @Override
    public String rewriteColumnType(ColumnType columnType, ColumnSize columnSize) {
        if (columnType == ColumnType.NUMBER || columnType == ColumnType.NUMERIC || columnType == ColumnType.DECIMAL) {
            // as one of the only relational databases out there, Oracle has a
            // NUMBER type. For this reason NUMBER would be replaced by the
            // super-type's logic, but we handle it specifically here.
            return super.rewriteColumnTypeInternal("NUMBER", columnSize.getColumnSizeContent());
        }
        if (columnType == ColumnType.BOOLEAN || columnType == ColumnType.BIT) {
            // Oracle has no boolean type, but recommends NUMBER(3) or CHAR(1).
            // For consistency with most other databases who have either a
            // boolean or a bit, we use the number variant because it's return
            // values (0 or 1) can be converted the most easily back to a
            // boolean.
            return "NUMBER(3)";
        }
        if (columnType == ColumnType.DOUBLE) {
            return "BINARY_DOUBLE";
        }
        if (columnType == ColumnType.FLOAT) {
            return "BINARY_FLOAT";
        }
        if (columnType == ColumnType.BINARY || columnType == ColumnType.VARBINARY) {
            return "RAW";
        }

        // following conversions based on
        // http://docs.oracle.com/cd/B19306_01/gateways.102/b14270/apa.htm
        if (columnType == ColumnType.TINYINT) {
            return "NUMBER(3)";
        }
        if (columnType == ColumnType.SMALLINT) {
            return "NUMBER(5)";
        }
        if (columnType == ColumnType.INTEGER) {
            return "NUMBER(10)";
        }
        if (columnType == ColumnType.BIGINT) {
            return "NUMBER(19)";
        }

        // Oracle has no "time only" data type but 'date' also includes time
        if (columnType == ColumnType.TIME) {
            super.rewriteColumnType(ColumnType.DATE, columnSize);
        }
        return super.rewriteColumnType(columnType, columnSize);
    }

    @Override
    public String rewriteFilterItem(final FilterItem item) {
        if (item.getOperand() instanceof String && item.getOperand().equals("")) {
            // In Oracle empty strings are treated as null. Typical SQL constructs with an empty string do not work.
            return super.rewriteFilterItem(new FilterItem(item.getSelectItem(), item.getOperator(), null));
        } else {
            return super.rewriteFilterItem(item);
        }
    }
}
