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
package com.redshoes.metamodel.fixedwidth;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.schema.naming.CustomColumnNamingStrategy;
import com.redshoes.metamodel.util.UrlResource;

import junit.framework.TestCase;

public class FixedWidthDataContextTest extends TestCase {

    public void testEmptyFile() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/empty_file.txt"),
                new FixedWidthConfiguration(10));
        assertEquals(2, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables().get(0);
        assertEquals("empty_file.txt", table.getName());
        assertEquals(0, table.getColumnCount());
    }

    public void testEmptyFileNoHeaderLine() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/empty_file.txt"),
                new FixedWidthConfiguration(FixedWidthConfiguration.NO_COLUMN_NAME_LINE, "UTF8", 10));
        assertEquals(2, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables().get(0);
        assertEquals("empty_file.txt", table.getName());
        assertEquals(0, table.getColumnCount());
    }

    public void testUnexistingHeaderLine() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple1.txt"),
                new FixedWidthConfiguration(20, "UTF8", 10));
        assertEquals(2, dc.getDefaultSchema().getTableCount());

        Table table = dc.getDefaultSchema().getTables().get(0);
        assertEquals("example_simple1.txt", table.getName());
        assertEquals(0, table.getColumnCount());
    }

    public void testExampleSimple1() throws Exception {
        FixedWidthConfiguration conf = new FixedWidthConfiguration(10);
        FixedWidthDataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple1.txt"), conf);

        String[] schemaNames = dc.getSchemaNames().toArray(new String[0]);
        assertEquals(2, schemaNames.length);
        assertEquals("[information_schema, resources]", Arrays.toString(schemaNames));

        Schema schema = dc.getDefaultSchema();
        assertEquals("Schema[name=resources]", schema.toString());

        assertEquals(2, schema.getTableCount());

        Table table = schema.getTableByName("example_simple1.txt");
        assertEquals("Table[name=example_simple1.txt,type=TABLE,remarks=null]", table.toString());

        assertEquals("[greeting, greeter]", Arrays.toString(table.getColumnNames().toArray()));
        assertEquals(10, table.getColumnByName("greeting").getColumnSize().intValue());
        assertEquals(10, table.getColumnByName("greeter").getColumnSize().intValue());

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("[hello, world]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[hi, there]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[howdy, partner]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testFailOnInconsistentWidth() throws Exception {
        FixedWidthConfiguration conf = new FixedWidthConfiguration(FixedWidthConfiguration.NO_COLUMN_NAME_LINE, "UTF8",
                10, true);
        FixedWidthDataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple1.txt"), conf);

        String[] schemaNames = dc.getSchemaNames().toArray(new String[0]);
        assertEquals(2, schemaNames.length);
        assertEquals("[information_schema, resources]", Arrays.toString(schemaNames));

        Schema schema = dc.getDefaultSchema();
        assertEquals("Schema[name=resources]", schema.toString());

        assertEquals(2, schema.getTableCount());

        Table table = schema.getTableByName("example_simple1.txt");
        assertEquals("Table[name=example_simple1.txt,type=TABLE,remarks=null]", table.toString());

        assertEquals("[A, B]", Arrays.toString(table.getColumnNames().toArray()));

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("greeting", ds.getRow().getValue(0));
        assertEquals("greeter", ds.getRow().getValue(1));

        try {
            ds.next();
            fail("Exception expected");
        } catch (InconsistentValueWidthException e) {
            assertEquals("Inconsistent row format of row no. 2.", e.getMessage());
            assertEquals("[hello, world]", Arrays.toString(e.getProposedRow().getValues()));
        }
        assertTrue(ds.next());
        assertEquals("[hi, there]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[howdy, partner]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testVaryingValueLengthsCorrect() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple2.txt"),
                new FixedWidthConfiguration(new int[] { 1, 8, 7 }));
        Table table = dc.getDefaultSchema().getTables().get(0);
        assertEquals("[i, greeting, greeter]", Arrays.toString(table.getColumnNames().toArray()));

        assertEquals(1, table.getColumnByName("i").getColumnSize().intValue());
        assertEquals(8, table.getColumnByName("greeting").getColumnSize().intValue());
        assertEquals(7, table.getColumnByName("greeter").getColumnSize().intValue());

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("[1, hello, world]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[2, hi, there]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[3, howdy, partner]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testVaryingValueLengthsTooShortLength() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple2.txt"),
                new FixedWidthConfiguration(0, "UTF8", new int[] { 1, 5, 7 }, true));
        try {
            dc.getDefaultSchema().getTables();
            fail("Exception expected");
        } catch (InconsistentValueWidthException e) {
            assertEquals("Inconsistent row format of row no. 1.", e.getMessage());
            assertEquals("igreetinggreeter", e.getSourceLine());
            assertEquals("[i, greet, inggree]", Arrays.toString(e.getSourceResult()));
        }
    }

    public void testVaryingValueLengthsTooShortLengthErrorTolerant() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple2.txt"),
                new FixedWidthConfiguration(FixedWidthConfiguration.DEFAULT_COLUMN_NAME_LINE, "UTF8", new int[] { 1, 5,
                        7 }, false));

        Table table = dc.getDefaultSchema().getTables().get(0);
        assertEquals("[i, greet, inggree]", Arrays.toString(table.getColumnNames().toArray()));

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("[1, hello, worl]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[2, hi, ther]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[3, howdy, part]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testVaryingValueLengthsTooLongLength() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple2.txt"),
                new FixedWidthConfiguration(0, "UTF8", new int[] { 1, 8, 9 }, true));

        try {
            dc.getDefaultSchema().getTables();
            fail("Exception expected");
        } catch (InconsistentValueWidthException e) {
            assertEquals("Inconsistent row format of row no. 1.", e.getMessage());
            assertEquals("igreetinggreeter", e.getSourceLine());
            assertEquals("[i, greeting, greeter]", Arrays.toString(e.getSourceResult()));
        }
    }

    public void testVaryingValueLengthsTooLongLengthErrorTolerant() throws Exception {
        DataContext dc = new FixedWidthDataContext(new File("src/test/resources/example_simple2.txt"),
                new FixedWidthConfiguration(FixedWidthConfiguration.DEFAULT_COLUMN_NAME_LINE, "UTF8", new int[] { 1, 8,
                        9 }, false));

        Table table = dc.getDefaultSchema().getTables().get(0);
        assertEquals("[i, greeting, greeter]", Arrays.toString(table.getColumnNames().toArray()));

        Query q = dc.query().from(table).select(table.getColumns()).toQuery();
        DataSet ds = dc.executeQuery(q);

        assertTrue(ds.next());
        assertEquals("[1, hello, world]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[2, hi, there]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[3, howdy, partner]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
    }

    public void testCustomColumnNames() throws Exception {
        final String firstColumnName = "first";
        final String secondColumnName = "second";

        final FixedWidthConfiguration configuration = new FixedWidthConfiguration(
                FixedWidthConfiguration.DEFAULT_COLUMN_NAME_LINE, new CustomColumnNamingStrategy(firstColumnName,
                        secondColumnName), "UTF8", new int[] { 10, 10 }, true);

        final DataContext dataContext = new FixedWidthDataContext(new File("src/test/resources/example_simple1.txt"),
                configuration);
        final Table table = dataContext.getDefaultSchema().getTable(0);

        assertNotNull(table.getColumnByName(firstColumnName));
        assertNotNull(table.getColumnByName(secondColumnName));
    }

    public void testUrlResource() throws MalformedURLException {
        final URL url = new URL("http://localhost:8080/fixed-width.txt");
        final DataContext dataContext = new FixedWidthDataContext(new ByteUrlResource(url),
                new EbcdicConfiguration(FixedWidthConfiguration.DEFAULT_COLUMN_NAME_LINE, "UTF8", 4, false, true,
                        true));
        assertNotNull(dataContext.getSchemaByName("localhost:8080"));
    }

    private static class ByteUrlResource extends UrlResource {
        public ByteUrlResource(final URL url) {
            super(url);
        }

        @Override
        public InputStream read() {
            // any InputStream that can not be cast to BufferedInputStream
            return new ByteArrayInputStream("test-data".getBytes());
        }
    }
}
