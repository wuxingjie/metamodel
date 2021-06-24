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
package com.redshoes.metamodel.excel;

import com.redshoes.metamodel.create.AbstractTableCreationBuilder;
import com.redshoes.metamodel.create.TableCreationBuilder;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.MutableSchema;
import com.redshoes.metamodel.schema.MutableTable;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;

/**
 * {@link TableCreationBuilder} implementation for Excel spreadsheets.
 */
final class ExcelTableCreationBuilder extends AbstractTableCreationBuilder<ExcelUpdateCallback> {

    public ExcelTableCreationBuilder(ExcelUpdateCallback updateCallback, Schema schema, String name) {
        super(updateCallback, schema, name);
    }

    @Override
    public Table execute() {
        final ExcelUpdateCallback updateCallback = getUpdateCallback();
        final MutableTable table = getTable();

        final Sheet sheet = updateCallback.createSheet(table.getName());

        final int lineNumber = updateCallback.getConfiguration().getColumnNameLineNumber();
        if (lineNumber != ExcelConfiguration.NO_COLUMN_NAME_LINE) {
            final int zeroBasedLineNumber = lineNumber - 1;
            final Row row = sheet.createRow(zeroBasedLineNumber);
            for (final Column column : table.getColumns()) {
                final int columnNumber = column.getColumnNumber();
                row.createCell(columnNumber).setCellValue(column.getName());
            }
        }

        final MutableSchema schema = (MutableSchema) getSchema();
        schema.addTable((MutableTable) table);
        return table;
    }
}
