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
package com.redshoes.metamodel.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.data.AbstractDataSet;
import com.redshoes.metamodel.data.DataSetHeader;
import com.redshoes.metamodel.data.Row;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.util.FileHelper;

import com.opencsv.ICSVParser;

/**
 * A specialized DataSet implementation for the CSV module under circumstances
 * where multiline values are disabled. In this case we can use a optimized
 * CSVParser and also lazy evaluate lines read from the file.
 */
final class SingleLineCsvDataSet extends AbstractDataSet {

    private final BufferedReader _reader;
    private final int _columnsInTable;
    private final boolean _failOnInconsistentRowLength;
    private final CsvParserBuilder _csvParserBuilder;
    
    private volatile int _rowNumber;
    private volatile Integer _rowsRemaining;
    private volatile Row _row;

    public SingleLineCsvDataSet(final BufferedReader reader, final List<Column> columns, final Integer maxRows,
                                final int columnsInTable, final CsvConfiguration csvConfiguration) {
        super(columns.stream().map(SelectItem::new).collect(Collectors.toList()));
        _reader = reader;
        _columnsInTable = columnsInTable;
        _failOnInconsistentRowLength = csvConfiguration.isFailOnInconsistentRowLength();
        _rowNumber = 0;
        _rowsRemaining = maxRows;
        _csvParserBuilder = new CsvParserBuilder(csvConfiguration);
    }

    @Override
    public void close() {
        FileHelper.safeClose(_reader);
        _row = null;
        _rowsRemaining = null;
    }

    @Override
    public boolean next() {
        if (_rowsRemaining != null && _rowsRemaining > 0) {
            _rowsRemaining--;
            return nextInternal();
        } else if (_rowsRemaining == null) {
            return nextInternal();
        } else {
            return false;
        }
    }

    @Override
    protected DataSetHeader getHeader() {
        // re-make this method protected so that it's visible for
        // SingleLineCsvRow.
        return super.getHeader();
    }

    protected boolean isFailOnInconsistentRowLength() {
        return _failOnInconsistentRowLength;
    }

    protected int getColumnsInTable() {
        return _columnsInTable;
    }

    protected ICSVParser getCsvParser() {
        return _csvParserBuilder.build();
    }

    public boolean nextInternal() {
        if (_reader == null) {
            return false;
        }

        try {
            final String line = _reader.readLine();
            if (line == null) {
                close();
                return false;
            }

            if ("".equals(line)) {
                // blank line - move to next line
                return nextInternal();
            }

            _rowNumber++;
            _row = new SingleLineCsvRow(this, line, _columnsInTable, _failOnInconsistentRowLength, _rowNumber);
            return true;
        } catch (IOException e) {
            close();
            throw new MetaModelException("IOException occurred while reading next line of CSV resource", e);
        }
    }

    @Override
    public Row getRow() {
        return _row;
    }

}
