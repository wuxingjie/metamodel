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
package com.redshoes.metamodel.arff;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.util.FileResource;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.ColumnTypeImpl;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.schema.TableType;
import org.junit.Test;

public class ArffDataContextTest {

    private final File wekaDataDir = new File("src/test/resources/weka-data");

    // generic test case that ensures we're able to load all the WEKA example files
    @Test
    public void testReadAllWekaArffExamples() {
        final File[] files = wekaDataDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".arff");
            }
        });

        assertTrue(files.length > 1);

        final AtomicInteger rowCounter = new AtomicInteger();
        final Set<ColumnType> observedColumnTypes = new HashSet<>();

        for (File file : files) {
            final ArffDataContext dc = new ArffDataContext(new FileResource(file));
            final Schema schema = dc.getDefaultSchema();
            assertNotNull(schema.getName());
            final List<Table> tables = schema.getTables(TableType.TABLE);
            assertEquals(1, tables.size());
            final List<Column> columns = tables.get(0).getColumns();
            assertFalse("No columns defined in file: " + file, columns.isEmpty());
            for (Column column : columns) {
                observedColumnTypes.add(column.getType());
            }

            try (DataSet dataSet = dc.query().from(tables.get(0)).selectAll().execute()) {
                while (dataSet.next()) {
                    assertNotNull(dataSet.getRow());
                    rowCounter.incrementAndGet();
                }
            }
        }

        assertTrue(observedColumnTypes.size() > 1);
        assertTrue(observedColumnTypes.contains(ColumnTypeImpl.STRING));
        assertTrue(observedColumnTypes.contains(ColumnTypeImpl.NUMBER));
        assertTrue(rowCounter.get() > 10_000);
    }

    // test case that checks our ability to parse column types and names from a specific file.
    @Test
    public void testReadStructureOfHypothyroid() {
        final File file = new File(wekaDataDir, "hypothyroid.arff");
        final ArffDataContext dc = new ArffDataContext(new FileResource(file));
        final Schema schema = dc.getDefaultSchema();
        final Table table = schema.getTable(0);
        assertEquals("hypothyroid", table.getName());

        final Column ageColumn = table.getColumnByName("age");
        assertNotNull(ageColumn);
        assertEquals(ColumnType.INTEGER, ageColumn.getType());
        assertEquals("integer", ageColumn.getRemarks());

        final Column sexColumn = table.getColumnByName("sex");
        assertNotNull(sexColumn);
        assertEquals(ColumnType.STRING, sexColumn.getType());
        assertEquals("{ F, M}", sexColumn.getRemarks());

        final Column tshColumn = table.getColumnByName("TSH measured");
        assertNotNull(tshColumn);
        assertEquals(ColumnType.STRING, tshColumn.getType());
        assertEquals("{ t, f}", tshColumn.getRemarks());
    }

    @Test
    public void testReadDataOfHypothyroid() {
        final File file = new File(wekaDataDir, "hypothyroid.arff");
        final ArffDataContext dc = new ArffDataContext(new FileResource(file));

        final DataSet dataSet =
                dc.query().from("hypothyroid").select("Class", "age", "sex", "TSH measured").limit(3).execute();
        try {
            assertTrue(dataSet.next());
            assertEquals("Row[values=[negative, 41, F, t]]", dataSet.getRow().toString());
            assertTrue(dataSet.next());
            assertEquals("Row[values=[negative, 23, F, t]]", dataSet.getRow().toString());
            assertTrue(dataSet.next());
            assertEquals("Row[values=[negative, 46, M, t]]", dataSet.getRow().toString());
            final Object ageValue = dataSet.getRow().getValue(1);
            assertEquals(Integer.class, ageValue.getClass());
            assertFalse(dataSet.next());
        } finally {
            dataSet.close();
        }
    }
}
