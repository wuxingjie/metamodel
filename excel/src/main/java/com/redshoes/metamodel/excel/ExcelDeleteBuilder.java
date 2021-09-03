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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.data.DataSetHeader;
import com.redshoes.metamodel.data.DefaultRow;
import com.redshoes.metamodel.data.SimpleDataSetHeader;
import com.redshoes.metamodel.delete.AbstractRowDeletionBuilder;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.schema.Table;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

final class ExcelDeleteBuilder extends AbstractRowDeletionBuilder {

    private final ExcelUpdateCallback _updateCallback;

    public ExcelDeleteBuilder(ExcelUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        // close the update callback will flush any changes
        _updateCallback.close();

        // read the workbook without streaming, since this will not wrap it in a
        // streaming workbook implementation (which do not support random
        // accessing rows).
        final Workbook workbook = _updateCallback.getWorkbook(false);

        final String tableName = getTable().getName();
        final List<SelectItem> selectItems = getTable().getColumns().stream().map(SelectItem::new).collect(Collectors.toList());
        final DataSetHeader header = new SimpleDataSetHeader(selectItems);
        final Sheet sheet = workbook.getSheet(tableName);

        final Iterator<Row> rowIterator = ExcelUtils.getRowIterator(sheet, _updateCallback.getConfiguration(), true);
        final List<Row> rowsToDelete = new ArrayList<Row>();
        while (rowIterator.hasNext()) {
            final Row excelRow = rowIterator.next();
            final DefaultRow row = ExcelUtils.createRow(workbook, excelRow, header);

            final boolean deleteRow = deleteRow(row);
            if (deleteRow) {
                rowsToDelete.add(excelRow);
            }
        }

        // reverse the list to not mess up any row numbers
        Collections.reverse(rowsToDelete);

        for (Row row : rowsToDelete) {
            sheet.removeRow(row);
        }
    }
}
