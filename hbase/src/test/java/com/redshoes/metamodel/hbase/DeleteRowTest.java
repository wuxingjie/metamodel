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
package com.redshoes.metamodel.hbase;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import com.redshoes.metamodel.insert.RowInsertionBuilder;
import com.redshoes.metamodel.schema.MutableTable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DeleteRowTest extends HBaseUpdateCallbackTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Delete is supported
     */
    @Test
    public void testDeleteSupported() {
        assertTrue(getUpdateCallback().isDeleteSupported());
    }

    /**
     * Having the table type wrong, should throw an exception
     */
    @Test
    public void testTableWrongType() {
        final MutableTable mutableTable = new MutableTable();

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Not an HBase table: " + mutableTable);

        getUpdateCallback().deleteFrom(mutableTable);
    }

    /**
     * Creating a HBaseRowDeletionBuilder with the DataContext null, should throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testDataContextNullAtBuilder() throws IOException {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("DataContext cannot be null");
        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);
        new HBaseRowDeletionBuilder(null, existingTable);
    }

    /**
     * Not setting the rowkey, should throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testNotSettingRowkey() throws IOException {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("HBase currently only supports deleting items by their row key.");

        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);
        getUpdateCallback().deleteFrom(existingTable).execute();
    }

    /**
     * Goodflow. Deleting a row, that doesn't exist, should not throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testDeletingNotExistingRow() throws IOException {
        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);

        checkRows(false, false);
        final HBaseRowDeletionBuilder rowDeletionBuilder = (HBaseRowDeletionBuilder) getUpdateCallback().deleteFrom(
                existingTable);
        rowDeletionBuilder.where(HBaseDataContext.FIELD_ID).eq(RK_1);
        rowDeletionBuilder.execute();
        checkRows(false, false);
    }

    /**
     * Deleting a row, which has an empty rowKey value, should throw an exception
     *
     * @throws IOException
     */
    @Test
    public void testUsingAnEmptyRowKeyValue() throws IOException {
        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);

        checkRows(false, false);
        final HBaseRowDeletionBuilder rowDeletionBuilder = (HBaseRowDeletionBuilder) getUpdateCallback().deleteFrom(
                existingTable);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Can't delete a row without an empty rowKey.");

        rowDeletionBuilder.where(HBaseDataContext.FIELD_ID).eq("");
        rowDeletionBuilder.execute();
    }

    /**
     * Goodflow. Deleting a row successfully.
     *
     * @throws IOException
     */
    @Test
    public void testDeleteRowSuccesfully() throws IOException {
        final HBaseTable existingTable = createAndAddTableToDatastore(TABLE_NAME, HBaseDataContext.FIELD_ID, CF_FOO,
                CF_BAR);
        final Map<HBaseColumn, Object> row = createRow(existingTable, HBaseDataContext.FIELD_ID, CF_FOO, CF_BAR, false);

        checkRows(false, false);
        final RowInsertionBuilder rowInsertionBuilder = getUpdateCallback().insertInto(existingTable);
        setValuesInInsertionBuilder(row, rowInsertionBuilder);
        rowInsertionBuilder.execute();
        checkRows(true, false);
        final HBaseRowDeletionBuilder rowDeletionBuilder = (HBaseRowDeletionBuilder) getUpdateCallback().deleteFrom(
                existingTable);
        rowDeletionBuilder.where(HBaseDataContext.FIELD_ID).eq(RK_1);
        rowDeletionBuilder.execute();
        checkRows(false, false);
    }
}
