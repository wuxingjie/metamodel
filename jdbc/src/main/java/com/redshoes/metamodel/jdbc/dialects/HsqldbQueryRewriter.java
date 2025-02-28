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

import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.query.SelectClause;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.jdbc.JdbcDataContext;

/**
 * Query rewriter for HSQLDB
 */
public class HsqldbQueryRewriter extends DefaultQueryRewriter {

    public HsqldbQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public String rewriteColumnType(ColumnType columnType, Integer columnSize,Integer decimalDigits) {
        if (columnType == ColumnType.BIT) {
            return "BOOLEAN";
        }
        if (columnType == ColumnType.BLOB) {
            return "LONGVARBINARY";
        }
        return super.rewriteColumnType(columnType, columnSize, decimalDigits);
    }

    @Override
    public boolean isFirstRowSupported(final Query query) {
        return true;
    }

    @Override
    public boolean isMaxRowsSupported() {
        return true;
    }

    @Override
    protected String rewriteSelectClause(Query query, SelectClause selectClause) {
        String result = super.rewriteSelectClause(query, selectClause);

        Integer firstRow = query.getFirstRow();
        Integer maxRows = query.getMaxRows();
        if (maxRows != null || firstRow != null) {
            if (maxRows == null) {
                maxRows = Integer.MAX_VALUE;
            }
            if (firstRow == null || firstRow <= 0) {
                result = "SELECT TOP " + maxRows + " " + result.substring(7);
            } else {
                final int offset = firstRow - 1;
                result = "SELECT LIMIT " + offset + " " + maxRows + " " + result.substring(7);
            }
        }

        return result;
    }

    @Override
    public String rewriteFilterItem(FilterItem item) {
        if (!item.isCompoundFilter()) {
            final SelectItem selectItem = item.getSelectItem();
            final Column column = selectItem.getColumn();
            if (column != null) {
                if (column.getType() == ColumnType.TIMESTAMP) {
                    // HSQLDB does not treat (TIMESTAMP 'yyyy-MM-dd hh:mm:ss')
                    // tokens correctly
                    String result = super.rewriteFilterItem(item);
                    int indexOfTimestamp = result.lastIndexOf("TIMESTAMP");
                    if (indexOfTimestamp != -1) {
                        result = result.substring(0, indexOfTimestamp)
                                + result.substring(indexOfTimestamp + "TIMESTAMP".length());
                    }
                    return result;
                }
            }
        }
        return super.rewriteFilterItem(item);
    }

    /**
     * HSQL converts all non-escaped characters to uppercases, this is prevented by always escaping
     */
    @Override
    public boolean needsQuoting(String alias, String identifierQuoteString) {
        final boolean containsLowerCase = identifierQuoteString.chars().anyMatch(Character::isLowerCase);

        return containsLowerCase || super.needsQuoting(alias, identifierQuoteString);
    }

}
