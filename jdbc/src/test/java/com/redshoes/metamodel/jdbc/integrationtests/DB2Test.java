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
package com.redshoes.metamodel.jdbc.integrationtests;

import static org.junit.Assert.*;

import java.util.Arrays;

import com.redshoes.metamodel.jdbc.JdbcDataContext;
import com.redshoes.metamodel.jdbc.JdbcTestTemplates;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.query.Query;
import org.junit.Test;

/**
 * DB2 integration test. This is a read-only integration test, meant to be
 * modified for whatever server is available (even within Human Inference).
 */
public class DB2Test extends AbstractJdbIntegrationTest {

    @Override
    protected String getPropertyPrefix() {
        return "db2";
    }

    @Test
    public void testCreateInsertAndUpdate() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcTestTemplates.simpleCreateInsertUpdateAndDrop(getDataContext(), "metamodel_db2_test");
    }

    @Test
    public void testCompositePrimaryKeyCreation() throws Exception {
        if (!isConfigured()) {
            return;
        }

        JdbcTestTemplates.compositeKeyCreation(getDataContext(), "metamodel_test_composite_keys");
    }

    @Test
    public void testInterpretationOfNull() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcTestTemplates.interpretationOfNulls(getConnection());
    }

    @Test
    public void testDefaultSchema() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcDataContext dc = new JdbcDataContext(getConnection());
        Schema schema = dc.getDefaultSchema();
        assertEquals(getUsername().toUpperCase(), schema.getName());

        Table countryTable = schema.getTableByName("COUNTRY");
        assertNotNull(countryTable);

        DataSet ds = dc.query().from(countryTable).selectCount().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1008]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }

    @Test
    public void testMaxRowsOnly() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcDataContext dc = new JdbcDataContext(getConnection());
        Schema schema = dc.getDefaultSchema();
        System.out.println("Tables: " + Arrays.toString(schema.getTableNames().toArray()));

        Table countryTable = schema.getTableByName("COUNTRY");
        assertNotNull(countryTable);

        Query query = dc.query().from(countryTable).select("COUNTRYCODE").limit(200).toQuery();
        assertEquals("SELECT DB2INST1.\"COUNTRY\".\"COUNTRYCODE\" FROM DB2INST1.\"COUNTRY\" "
                + "FETCH FIRST 200 ROWS ONLY", dc.getQueryRewriter().rewriteQuery(query));

        DataSet ds = dc.executeQuery(query);
        for (int i = 0; i < 200; i++) {
            assertTrue(ds.next());
            assertEquals(1, ds.getRow().getValues().length);
        }
        assertFalse(ds.next());
        ds.close();
    }

    @Test
    public void testMaxRowsAndOffset() throws Exception {
        if (!isConfigured()) {
            return;
        }
        JdbcDataContext dc = new JdbcDataContext(getConnection());
        Schema schema = dc.getDefaultSchema();
        System.out.println("Tables: " + Arrays.toString(schema.getTableNames().toArray()));

        Table countryTable = schema.getTableByName("COUNTRY");
        assertNotNull(countryTable);

        Query query = dc.query().from(countryTable).select("COUNTRYCODE").limit(200).offset(200).toQuery();
        assertEquals(
                "SELECT metamodel_subquery.\"COUNTRYCODE\" FROM ("
                        + "SELECT DB2INST1.\"COUNTRY\".\"COUNTRYCODE\", ROW_NUMBER() OVER() AS metamodel_row_number FROM DB2INST1.\"COUNTRY\""
                        + ") metamodel_subquery WHERE metamodel_row_number BETWEEN 201 AND 400", dc.getQueryRewriter()
                        .rewriteQuery(query));

        DataSet ds = dc.executeQuery(query);
        for (int i = 0; i < 200; i++) {
            assertTrue(ds.next());
            assertEquals(1, ds.getRow().getValues().length);
        }
        assertFalse(ds.next());
        ds.close();
    }
}
