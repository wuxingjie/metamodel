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

import java.text.DateFormat;
import java.util.Date;

import com.redshoes.metamodel.jdbc.JdbcDataContext;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnSize;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.util.DateUtils;
import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.OperatorType;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.query.SelectClause;
import com.redshoes.metamodel.query.SelectItem;

public class SQLServerQueryRewriter extends OffsetFetchQueryRewriter {

    public static final int FIRST_FETCH_SUPPORTING_VERSION = 11;

    public SQLServerQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext, FIRST_FETCH_SUPPORTING_VERSION, true);
    }

    @Override
    public boolean isMaxRowsSupported() {
        return true;
    }

    /**
     * SQL server expects the fully qualified column name, including schema, in
     * select items.
     */
    @Override
    public boolean isSchemaIncludedInColumnPaths() {
        return true;
    }

    @Override
    protected String rewriteSelectClause(Query query, SelectClause selectClause) {
        String result = super.rewriteSelectClause(query, selectClause);

        Integer maxRows = query.getMaxRows();
        if (maxRows != null) {
            if (query.getSelectClause().isDistinct()) {
                result = "SELECT DISTINCT TOP " + maxRows + " " + result.substring("SELECT DISTINCT ".length());
            } else {
                result = "SELECT TOP " + maxRows + " " + result.substring("SELECT ".length());
            }
        }

        return result;
    }

    @Override
    public String rewriteColumnType(ColumnType columnType, ColumnSize columnSize) {
        if (columnType == ColumnType.DOUBLE) {
            return "FLOAT";
        }
        if (columnType == ColumnType.BOOLEAN) {
            return "BIT";
        }
        if (columnType.isLiteral() && columnSize.isEmpty()) {
            // SQL server provides the convenient MAX parameter. If not
            // specified, the default size of e.g. a VARCHAR is 1!
            return rewriteColumnTypeInternal(columnType.getName(), "MAX");
        }
        return super.rewriteColumnType(columnType, columnSize);
    }

    @Override
    public String rewriteFilterItem(FilterItem item) {
        if (item.isCompoundFilter()) {
            return super.rewriteFilterItem(item);
        }

        final SelectItem selectItem = item.getSelectItem();
        final Object operand = item.getOperand();
        final OperatorType operator = item.getOperator();

        if (selectItem == null || operand == null || operator == null) {
            return super.rewriteFilterItem(item);
        }

        final Column column = selectItem.getColumn();
        if (column == null) {
            return super.rewriteFilterItem(item);
        }

        if (operand instanceof Date) {
            final String nativeType = column.getNativeType();
            if ("TIMESTAMP".equalsIgnoreCase(nativeType) || "DATETIME".equalsIgnoreCase(nativeType)) {
                final StringBuilder sb = new StringBuilder();
                sb.append(selectItem.getSameQueryAlias(true));

                FilterItem.appendOperator(sb, operand, operator);

                final Date date = (Date) operand;

                final DateFormat format = DateUtils.createDateFormat("yyyyMMdd HH:mm:ss.SSS");
                final String dateTimeValue = "CAST('" + format.format(date) + "' AS DATETIME)";

                sb.append(dateTimeValue);
                return sb.toString();
            }
        }
        return super.rewriteFilterItem(item);
    }
}