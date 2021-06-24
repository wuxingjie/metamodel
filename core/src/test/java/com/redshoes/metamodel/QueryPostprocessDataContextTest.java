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
package com.redshoes.metamodel;

import java.nio.channels.UnsupportedAddressTypeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.swing.table.TableModel;

import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.data.DataSetHeader;
import com.redshoes.metamodel.data.DataSetTableModel;
import com.redshoes.metamodel.data.DefaultRow;
import com.redshoes.metamodel.data.EmptyDataSet;
import com.redshoes.metamodel.data.InMemoryDataSet;
import com.redshoes.metamodel.data.Row;
import com.redshoes.metamodel.data.SimpleDataSetHeader;
import com.redshoes.metamodel.query.CompiledQuery;
import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.FromItem;
import com.redshoes.metamodel.query.FunctionType;
import com.redshoes.metamodel.query.GroupByItem;
import com.redshoes.metamodel.query.JoinType;
import com.redshoes.metamodel.query.OperatorType;
import com.redshoes.metamodel.query.OrderByItem;
import com.redshoes.metamodel.query.OrderByItem.Direction;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.query.QueryParameter;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.MutableColumn;
import com.redshoes.metamodel.schema.MutableSchema;
import com.redshoes.metamodel.schema.MutableTable;
import com.redshoes.metamodel.schema.Relationship;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;

public class QueryPostprocessDataContextTest extends MetaModelTestCase {

    private final Schema schema = getExampleSchema();
    private final Table table1 = schema.getTableByName(TABLE_CONTRIBUTOR);
    private final Table table2 = schema.getTableByName(TABLE_ROLE);

    public void testSchemaTraversalWithAliasTable() {
        final MockUpdateableDataContext dc = new MockUpdateableDataContext();

        final Column column = dc.getColumnByQualifiedLabel("foo");
        assertEquals("table", column.getTable().getName());
    }

    public void testNoAliasTableWhenSystemPropertySet() {
        System.setProperty(QueryPostprocessDataContext.SYSTEM_PROPERTY_CREATE_DEFAULT_TABLE_ALIAS, "false");
        try {
            final MockUpdateableDataContext dc = new MockUpdateableDataContext();
            final List<Table> tables = dc.getDefaultSchema().getTables();
            assertEquals(1, tables.size());

            assertEquals("table", tables.get(0).getName());
        } finally {
            System.clearProperty(QueryPostprocessDataContext.SYSTEM_PROPERTY_CREATE_DEFAULT_TABLE_ALIAS);
        }
    }

    public void testNoAliasTableWhenConstructorArgSet() {
        final MockUpdateableDataContext dc = new MockUpdateableDataContext(false);
        final List<Table> tables = dc.getDefaultSchema().getTables();
        assertEquals(1, tables.size());

        assertEquals("table", tables.get(0).getName());
    }

    public void testAliasTableQueries() {
        final MockUpdateableDataContext dc = new MockUpdateableDataContext();
        final List<Table> tables = dc.getDefaultSchema().getTables();
        assertEquals(2, tables.size());

        final Query q0 = dc.query().from(tables.get(0)).selectAll().toQuery();
        assertEquals("SELECT table.foo, table.bar FROM schema.table", q0.toSql());

        final Query q1 = dc.parseQuery("SELECT * FROM default_table d");
        assertEquals("SELECT d.foo, d.bar FROM schema.default_table d", q1.toSql());

        final Query q2 = dc.parseQuery("SELECT * FROM default_table");
        assertEquals("SELECT default_table.foo, default_table.bar FROM schema.default_table", q2.toSql());

        final DataSet dataSet0 = dc.executeQuery(q0);
        final DataSet dataSet1 = dc.executeQuery(q1);
        final DataSet dataSet2 = dc.executeQuery(q2);

        Arrays.asList(dataSet0, dataSet1, dataSet2).forEach(ds -> {
            assertTrue(ds.next());
            assertEquals("Row[values=[1, hello]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[2, there]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertFalse(ds.next());
            ds.close();
        });

        assertEquals("Row[values=[3]]", MetaModelHelper
                .executeSingleRowQuery(dc, dc.parseQuery("SELECT COUNT(*) FROM default_table")).toString());
        assertEquals("Row[values=[1]]",
                MetaModelHelper
                        .executeSingleRowQuery(dc, dc.parseQuery("SELECT COUNT(*) FROM default_table WHERE foo = '2'"))
                        .toString());
    }

    public void testQueryMaxRows0() throws Exception {
        final MockDataContext dc = new MockDataContext("sch", "tab", "1");
        final Table table = dc.getDefaultSchema().getTable(0);
        final DataSet dataSet = dc.query().from(table).selectAll().limit(0).execute();
        assertTrue(dataSet instanceof EmptyDataSet);
        assertFalse(dataSet.next());
        dataSet.close();
    }

    // see issue METAMODEL-100
    public void testSelectFromColumnsWithSameName() throws Exception {
        final MutableTable table = new MutableTable("table");
        table.addColumn(new MutableColumn("foo", table).setColumnNumber(0));
        table.addColumn(new MutableColumn("foo", table).setColumnNumber(1));
        table.addColumn(new MutableColumn("bar", table).setColumnNumber(2));

        final QueryPostprocessDataContext dc = new QueryPostprocessDataContext() {
            @Override
            protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
                Object[] values = new Object[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    values[i] = columns.get(i).getColumnNumber();
                }
                DataSetHeader header =
                        new SimpleDataSetHeader(columns.stream().map(SelectItem::new).collect(Collectors.toList()));
                DefaultRow row = new DefaultRow(header, values);
                return new InMemoryDataSet(row);
            }

            @Override
            protected String getMainSchemaName() throws MetaModelException {
                return "sch";
            }

            @Override
            protected Schema getMainSchema() throws MetaModelException {
                MutableSchema schema = new MutableSchema(getMainSchemaName());
                schema.addTable(table);
                table.setSchema(schema);
                return schema;
            }
        };

        DataSet ds = dc.query().from(table).selectAll().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[0, 1, 2]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }

    public void testAggregateQueryNoWhereClause() throws Exception {
        MockDataContext dc = new MockDataContext("sch", "tab", "1");
        Table table = dc.getDefaultSchema().getTable(0);
        assertSingleRowResult("Row[values=[4]]", dc.query().from(table).selectCount().execute());
    }

    public void testAggregateQueryRegularWhereClause() throws Exception {
        MockDataContext dc = new MockDataContext("sch", "tab", "1");
        Table table = dc.getDefaultSchema().getTable(0);
        assertSingleRowResult("Row[values=[3]]",
                dc.query().from(table).selectCount().where("baz").eq("world").execute());
    }

    public void testApplyFunctionToNullValues() throws Exception {
        QueryPostprocessDataContext dataContext = new QueryPostprocessDataContext() {
            @Override
            public DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
                if (table == table1) {
                    List<Column> columns1 = table1.getColumns();
                    List<SelectItem> selectItems = columns1.stream().map(SelectItem::new).collect(Collectors.toList());
                    List<Object[]> data = new ArrayList<Object[]>();
                    data.add(new Object[] { 1, "no nulls", 1 });
                    data.add(new Object[] { 2, "onlynull", null });
                    data.add(new Object[] { 3, "mixed", null });
                    data.add(new Object[] { 4, "mixed", "" });
                    data.add(new Object[] { 5, "mixed", 2 });
                    data.add(new Object[] { 6, "mixed", " \n \t " });
                    if (maxRows != -1) {
                        for (int i = data.size() - 1; i >= maxRows; i--) {
                            data.remove(i);
                        }
                    }
                    return createDataSet(selectItems, data);
                }
                throw new IllegalArgumentException("This test only accepts table1 and table2");
            }

            @Override
            protected String getMainSchemaName() throws MetaModelException {
                return schema.getName();
            }

            @Override
            protected Schema getMainSchema() throws MetaModelException {
                return schema;
            }
        };

        DataSet dataSet = dataContext.query().from(TABLE_CONTRIBUTOR)
                .select(FunctionType.SUM, COLUMN_CONTRIBUTOR_COUNTRY).select(COLUMN_CONTRIBUTOR_NAME)
                .groupBy(COLUMN_CONTRIBUTOR_NAME).orderBy(COLUMN_CONTRIBUTOR_NAME).execute();
        assertTrue(dataSet.next());
        assertEquals("Row[values=[2.0, mixed]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[1.0, no nulls]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[0.0, onlynull]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
        dataSet.close();
    }

    public void testGroupByNulls() throws Exception {
        MockDataContext dc = new MockDataContext("sch", "tab", null);
        Table table = dc.getDefaultSchema().getTable(0);
        DataSet dataSet = dc.query().from(table).select(FunctionType.SUM, "foo").select("baz").groupBy("baz").execute();
        assertTrue(dataSet.next());
        assertEquals("Row[values=[7.0, world]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[3.0, null]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
        dataSet.close();
    }

    public void testNewAggregateFunctions() throws Exception {
        MockDataContext dc = new MockDataContext("sch", "tab", null);
        Table table = dc.getDefaultSchema().getTable(0);
        DataSet dataSet = dc.query().from(table).select(FunctionType.FIRST, "foo").select(FunctionType.LAST, "foo")
                .select(FunctionType.RANDOM, "foo").execute();
        assertTrue(dataSet.next());

        final Row row = dataSet.getRow();
        assertEquals("1", row.getValue(0));
        assertEquals("4", row.getValue(1));

        final Object randomValue = row.getValue(2);
        assertTrue(Arrays.asList("1", "2", "3", "4").contains(randomValue));

        assertFalse(dataSet.next());
        dataSet.close();
    }

    public void testAggregateQueryWhereClauseExcludingAll() throws Exception {
        MockDataContext dc = new MockDataContext("sch", "tab", "1");
        assertSingleRowResult("Row[values=[0]]",
                dc.query().from("tab").selectCount().where("baz").eq("non_existing_value").execute());
    }

    public void testMixedAggregateAndRawQueryOnEmptyTable() throws Exception {
        MockDataContext dc = new MockDataContext("sch", "tab", "1");
        Table emptyTable = dc.getTableByQualifiedLabel("an_empty_table");

        assertSingleRowResult("Row[values=[0, null]]", dc.query().from(emptyTable).selectCount().and("foo").execute());
    }

    private void assertSingleRowResult(String rowStr, DataSet ds) {
        assertTrue("DataSet had no rows", ds.next());
        Row row = ds.getRow();
        assertEquals(rowStr, row.toString());
        assertFalse("DataSet had more than a single row!", ds.next());
        ds.close();
    }

    public void testMixedAggregateAndRawQuery() throws Exception {
        MockDataContext dc = new MockDataContext("sch", "tab", "1");
        Table table = dc.getDefaultSchema().getTable(0);
        List<Column> columns = table.getColumns();

        Query query = dc.query().from(table).select(FunctionType.MAX, columns.get(0)).and(columns.get(1)).toQuery();
        assertEquals("SELECT MAX(tab.foo), tab.bar FROM sch.tab", query.toSql());

        DataSet ds = dc.executeQuery(query);
        assertTrue(ds.next());
        assertEquals("Row[values=[4, hello]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[4, 1]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[4, hi]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[4, yo]]", ds.getRow().toString());
        assertFalse(ds.next());
    }

    public void testScalarFunctionSelect() throws Exception {
        MockDataContext dc = new MockDataContext("sch", "tab", "1");
        Table table = dc.getDefaultSchema().getTable(0);

        Query query = dc.query().from(table).select("foo").select(FunctionType.TO_NUMBER, "foo").select("bar")
                .select(FunctionType.TO_STRING, "bar").select(FunctionType.TO_NUMBER, "bar").toQuery();
        assertEquals("SELECT tab.foo, TO_NUMBER(tab.foo), tab.bar, TO_STRING(tab.bar), TO_NUMBER(tab.bar) FROM sch.tab",
                query.toSql());

        DataSet ds = dc.executeQuery(query);
        assertTrue(ds.next());
        Row row;

        row = ds.getRow();
        assertEquals("Row[values=[1, 1, hello, hello, null]]", row.toString());
        Object value1 = row.getValue(1);
        assertEquals(Integer.class, value1.getClass());

        assertTrue(ds.next());

        row = ds.getRow();
        assertEquals("Row[values=[2, 2, 1, 1, 1]]", row.toString());
        Object value2 = row.getValue(1);
        assertEquals(Integer.class, value2.getClass());
        Object value3 = row.getValue(4);
        assertEquals(Integer.class, value3.getClass());

        assertTrue(ds.next());
        ds.close();
    }

    public void testScalarFunctionWhere() throws Exception {
        final MockDataContext dc = new MockDataContext("sch", "tab", "1");
        final Table table = dc.getDefaultSchema().getTable(0);

        final Query query = dc.query().from(table).select("foo").where(FunctionType.TO_NUMBER, "bar").eq(1).toQuery();
        assertEquals("SELECT tab.foo FROM sch.tab WHERE TO_NUMBER(tab.bar) = 1", query.toSql());

        final DataSet ds = dc.executeQuery(query);
        assertTrue(ds.next());
        final Row row = ds.getRow();
        assertEquals("Row[values=[2]]", row.toString());

        assertFalse(ds.next());
        ds.close();
    }

    public void testSelectItemReferencesToFromItems() throws Exception {
        MockDataContext dc = new MockDataContext("sch", "tab", "1");

        Table table = dc.getDefaultSchema().getTable(0);

        Query q = new Query();
        FromItem fromItem1 = q.from(table, "t1").getFromClause().getItem(0);
        FromItem fromItem2 = q.from(table, "t2").getFromClause().getItem(1);
        q.select(table.getColumnByName("foo"), fromItem1);
        q.select(table.getColumnByName("foo"), fromItem2);
        q.where(q.getSelectClause().getItem(0), OperatorType.EQUALS_TO, "2");
        assertEquals("SELECT t1.foo, t2.foo FROM sch.tab t1, sch.tab t2 WHERE t1.foo = '2'", q.toSql());

        DataSet ds = dc.executeQuery(q);
        List<SelectItem> selectItems = ds.getSelectItems();
        assertEquals(2, selectItems.size());
        assertEquals("t1.foo", selectItems.get(0).toSql());
        assertEquals("t2.foo", selectItems.get(1).toSql());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 1]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 2]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 3]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 4]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }

    private DataContext getDataContext() {
        QueryPostprocessDataContext dataContext = new QueryPostprocessDataContext() {

            @Override
            public DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
                if (table == table1) {
                    List<Column> columns1 = table1.getColumns();
                    List<SelectItem> selectItems = columns1.stream().map(SelectItem::new).collect(Collectors.toList());
                    List<Object[]> data = new ArrayList<Object[]>();
                    data.add(new Object[] { 1, "kasper", "denmark" });
                    data.add(new Object[] { 2, "asbjorn", "denmark" });
                    data.add(new Object[] { 3, "johny", "israel" });
                    data.add(new Object[] { 4, "daniel", "canada" });
                    data.add(new Object[] { 5, "sasidhar", "unknown" });
                    data.add(new Object[] { 6, "jesper", "denmark" });
                    if (maxRows != -1) {
                        for (int i = data.size() - 1; i >= maxRows; i--) {
                            data.remove(i);
                        }
                    }
                    return createDataSet(selectItems, data);
                } else if (table == table2) {
                    List<SelectItem> selectItems =
                            table2.getColumns().stream().map(SelectItem::new).collect(Collectors.toList());
                    List<Object[]> data = new ArrayList<Object[]>();
                    data.add(new Object[] { 1, 1, "founder" });
                    data.add(new Object[] { 1, 1, "developer" });
                    data.add(new Object[] { 1, 2, "developer" });
                    data.add(new Object[] { 2, 1, "developer" });
                    data.add(new Object[] { 2, 3, "developer" });
                    data.add(new Object[] { 4, 1, "advisor" });
                    data.add(new Object[] { 5, 2, "developer" });
                    data.add(new Object[] { 6, 1, "founder" });
                    if (maxRows != -1) {
                        for (int i = data.size() - 1; i >= maxRows; i--) {
                            data.remove(i);
                        }
                    }
                    return createDataSet(selectItems, data);
                }
                throw new IllegalArgumentException("This test only accepts table1 and table2");
            }

            @Override
            protected String getMainSchemaName() throws MetaModelException {
                return schema.getName();
            }

            @Override
            protected Schema getMainSchema() throws MetaModelException {
                return schema;
            }
        };
        return dataContext;
    }

    public void testDistinct() throws Exception {

        Column roleColumn = table2.getColumnByName(COLUMN_ROLE_ROLE_NAME);

        Query q = new Query().select(roleColumn).from(table2).orderBy(roleColumn);
        q.getSelectClause().setDistinct(true);

        DataContext dc = getDataContext();
        DataSet data = dc.executeQuery(q);
        assertTrue(data.next());
        assertEquals("advisor", data.getRow().getValue(roleColumn));
        assertTrue(data.next());
        assertEquals("developer", data.getRow().getValue(roleColumn));
        assertTrue(data.next());
        assertEquals("founder", data.getRow().getValue(roleColumn));
        assertFalse(data.next());
    }

    public void testInformationSchema() throws Exception {
        DataContext dc = getDataContext();
        assertEquals("[information_schema, MetaModelSchema]", Arrays.toString(dc.getSchemaNames().toArray()));
        Schema informationSchema = dc.getSchemaByName("information_schema");
        assertEquals(
                "[Table[name=tables,type=TABLE,remarks=null], Table[name=columns,type=TABLE,remarks=null], Table[name=relationships,type=TABLE,remarks=null]]",
                Arrays.toString(informationSchema.getTables().toArray()));
        assertEquals(
                "[Relationship[primaryTable=tables,primaryColumns=[name],foreignTable=columns,foreignColumns=[table]], "
                        + "Relationship[primaryTable=tables,primaryColumns=[name],foreignTable=relationships,foreignColumns=[primary_table]], "
                        + "Relationship[primaryTable=tables,primaryColumns=[name],foreignTable=relationships,foreignColumns=[foreign_table]], "
                        + "Relationship[primaryTable=columns,primaryColumns=[name],foreignTable=relationships,foreignColumns=[primary_column]], "
                        + "Relationship[primaryTable=columns,primaryColumns=[name],foreignTable=relationships,foreignColumns=[foreign_column]]]",
                Arrays.toString(informationSchema.getRelationships().toArray()));
        Table tablesTable = informationSchema.getTableByName("tables");
        assertEquals("[Column[name=name,columnNumber=0,type=VARCHAR,nullable=false,nativeType=null,columnSize=null], "
                + "Column[name=type,columnNumber=1,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=num_columns,columnNumber=2,type=INTEGER,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=remarks,columnNumber=3,type=VARCHAR,nullable=true,nativeType=null,columnSize=null]]",
                Arrays.toString(tablesTable.getColumns().toArray()));
        Table columnsTable = informationSchema.getTableByName("columns");
        assertEquals("[Column[name=name,columnNumber=0,type=VARCHAR,nullable=false,nativeType=null,columnSize=null], "
                + "Column[name=type,columnNumber=1,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=native_type,columnNumber=2,type=VARCHAR,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=size,columnNumber=3,type=INTEGER,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=nullable,columnNumber=4,type=BOOLEAN,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=indexed,columnNumber=5,type=BOOLEAN,nullable=true,nativeType=null,columnSize=null], "
                + "Column[name=table,columnNumber=6,type=VARCHAR,nullable=false,nativeType=null,columnSize=null], "
                + "Column[name=remarks,columnNumber=7,type=VARCHAR,nullable=true,nativeType=null,columnSize=null]]",
                Arrays.toString(columnsTable.getColumns().toArray()));
        Table relationshipsTable = informationSchema.getTableByName("relationships");
        assertEquals(
                "[Column[name=primary_table,columnNumber=0,type=VARCHAR,nullable=false,nativeType=null,columnSize=null], "
                        + "Column[name=primary_column,columnNumber=1,type=VARCHAR,nullable=false,nativeType=null,columnSize=null], "
                        + "Column[name=foreign_table,columnNumber=2,type=VARCHAR,nullable=false,nativeType=null,columnSize=null], "
                        + "Column[name=foreign_column,columnNumber=3,type=VARCHAR,nullable=false,nativeType=null,columnSize=null]]",
                Arrays.toString(relationshipsTable.getColumns().toArray()));

        DataSet dataSet = dc.query().from(tablesTable).select(tablesTable.getColumns()).execute();
        assertTrue(dataSet.next());
        assertEquals("Row[values=[contributor, TABLE, 3, null]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[project, TABLE, 4, null]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[role, TABLE, 3, null]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[project_contributor, VIEW, 3, null]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
        dataSet.close();

        Relationship relationship = tablesTable.getRelationships(columnsTable).iterator().next();
        FromItem joinFromItem = new FromItem(JoinType.INNER, relationship);
        Query q = new Query().select(tablesTable.getColumnByName("name")).select(columnsTable.getColumnByName("name"))
                .select(columnsTable.getBooleanColumns()).from(joinFromItem);

        assertEquals("SELECT tables.name, columns.name, columns.nullable, columns.indexed "
                + "FROM information_schema.tables INNER JOIN information_schema.columns "
                + "ON tables.name = columns.table", q.toString());

        dataSet = dc.executeQuery(q);
        assertTrue(dataSet.next());
        assertEquals("Row[values=[contributor, contributor_id, false, true]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[contributor, name, false, false]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[contributor, country, true, false]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[project, project_id, false, false]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[project, name, false, false]]", dataSet.getRow().toString());
        dataSet.close();
    }

    public void testOrderByWithoutSelecting() throws Exception {
        Query q = new Query();
        q.from(new FromItem(table2).setAlias("r"));
        Column roleColumn = table2.getColumnByName(COLUMN_ROLE_ROLE_NAME);
        Column projectIdColumn = table2.getColumnByName(COLUMN_ROLE_PROJECT_ID);
        q.select(new SelectItem(projectIdColumn));
        q.orderBy(roleColumn);
        assertEquals("SELECT r.project_id FROM MetaModelSchema.role r ORDER BY r.name ASC", q.toString());

        DataContext dc = getDataContext();
        DataSet data = dc.executeQuery(q);
        assertEquals(1, data.getSelectItems().size());

        TableModel tableModel = new DataSetTableModel(data);

        // should correspond to these lines:

        // data.add(new Object[] { 4, 1, "advisor" });
        // data.add(new Object[] { 1, 1, "developer" });
        // data.add(new Object[] { 1, 2, "developer" });
        // data.add(new Object[] { 2, 1, "developer" });
        // data.add(new Object[] { 2, 3, "developer" });
        // data.add(new Object[] { 5, 2, "developer" });
        // data.add(new Object[] { 1, 1, "founder" });
        // data.add(new Object[] { 6, 1, "founder" });

        assertEquals(8, tableModel.getRowCount());
        assertEquals(1, tableModel.getColumnCount());
        assertEquals(1, tableModel.getValueAt(0, 0));
        assertEquals(1, tableModel.getValueAt(1, 0));
        assertEquals(2, tableModel.getValueAt(2, 0));
        assertEquals(1, tableModel.getValueAt(3, 0));
        assertEquals(3, tableModel.getValueAt(4, 0));
        assertEquals(2, tableModel.getValueAt(5, 0));
        assertEquals(1, tableModel.getValueAt(6, 0));
        assertEquals(1, tableModel.getValueAt(7, 0));
    }

    public void testGroupByWithoutSelecting() throws Exception {
        Query q = new Query();
        q.from(new FromItem(table2).setAlias("r"));
        Column roleColumn = table2.getColumnByName(COLUMN_ROLE_ROLE_NAME);
        Column projectIdColumn = table2.getColumnByName(COLUMN_ROLE_PROJECT_ID);
        q.select(new SelectItem(FunctionType.SUM, projectIdColumn));
        q.groupBy(new GroupByItem(new SelectItem(roleColumn)));
        q.orderBy(roleColumn);
        assertEquals("SELECT SUM(r.project_id) FROM MetaModelSchema.role r GROUP BY r.name ORDER BY r.name ASC",
                q.toString());

        DataContext dc = getDataContext();

        DataSet data = dc.executeQuery(q);
        assertEquals(1, data.getSelectItems().size());
        assertEquals("SUM(r.project_id)", data.getSelectItems().get(0).toString());

        TableModel tableModel = new DataSetTableModel(data);
        assertEquals(3, tableModel.getRowCount());
        assertEquals(1, tableModel.getColumnCount());
        assertEquals(1.0, tableModel.getValueAt(0, 0));
        assertEquals(9.0, tableModel.getValueAt(1, 0));
        assertEquals(2.0, tableModel.getValueAt(2, 0));

        q = dc.query().from(table2).select("name").orderBy("name").toQuery();
        q.getSelectClause().setDistinct(true);

        tableModel = new DataSetTableModel(dc.executeQuery(q));
        assertEquals(3, tableModel.getRowCount());
        assertEquals(1, tableModel.getColumnCount());
        assertEquals("advisor", tableModel.getValueAt(0, 0));
        assertEquals("developer", tableModel.getValueAt(1, 0));
        assertEquals("founder", tableModel.getValueAt(2, 0));
    }

    public void testSimpleGroupBy() throws Exception {
        Query q = new Query();
        q.from(new FromItem(table2).setAlias("r"));
        Column roleColumn = table2.getColumnByName(COLUMN_ROLE_ROLE_NAME);
        q.select(new SelectItem(roleColumn));
        q.groupBy(new GroupByItem(new SelectItem(roleColumn)));
        assertEquals("SELECT r.name FROM MetaModelSchema.role r GROUP BY r.name", q.toString());

        DataContext dc = getDataContext();
        DataSet data = dc.executeQuery(q);
        assertEquals(1, data.getSelectItems().size());
        assertEquals("r.name", data.getSelectItems().get(0).toString());
        TableModel tableModel = new DataSetTableModel(data);
        assertEquals(3, tableModel.getRowCount());

        q.select(new SelectItem(FunctionType.COUNT, "*", "c"));
        q.where(new FilterItem(new SelectItem(roleColumn), OperatorType.EQUALS_TO, "founder"));
        data = dc.executeQuery(q);
        assertEquals(2, data.getSelectItems().size());
        assertEquals("r.name", data.getSelectItems().get(0).toString());
        assertEquals("COUNT(*) AS c", data.getSelectItems().get(1).toString());
        tableModel = new DataSetTableModel(data);
        assertEquals(1, tableModel.getRowCount());
        assertEquals("founder", tableModel.getValueAt(0, 0));
        assertEquals(2l, tableModel.getValueAt(0, 1));

        q.select(new SelectItem(FunctionType.SUM, table2.getColumn(0)));
        assertEquals(
                "SELECT r.name, COUNT(*) AS c, SUM(r.contributor_id) FROM MetaModelSchema.role r WHERE r.name = 'founder' GROUP BY r.name",
                q.toString());
        data = dc.executeQuery(q);
        assertEquals(3, data.getSelectItems().size());
        assertEquals("r.name", data.getSelectItems().get(0).toString());
        assertEquals("COUNT(*) AS c", data.getSelectItems().get(1).toString());
        assertEquals("SUM(r.contributor_id)", data.getSelectItems().get(2).toString());
        tableModel = new DataSetTableModel(data);
        assertEquals(1, tableModel.getRowCount());
        assertEquals("founder", tableModel.getValueAt(0, 0));
        assertEquals(2l, tableModel.getValueAt(0, 1));
        assertEquals(7.0, tableModel.getValueAt(0, 2));
    }

    public void testSimpleHaving() throws Exception {
        Query q = new Query();
        q.from(table2, "c");
        Column roleColumn = table2.getColumnByName(COLUMN_ROLE_ROLE_NAME);
        Column contributorIdColumn = table2.getColumnByName(COLUMN_ROLE_CONTRIBUTOR_ID);

        q.groupBy(roleColumn);
        SelectItem countSelectItem = new SelectItem(FunctionType.COUNT, contributorIdColumn).setAlias("my_count");
        q.select(new SelectItem(roleColumn), countSelectItem);
        q.having(new FilterItem(countSelectItem, OperatorType.GREATER_THAN, 1));
        q.orderBy(new OrderByItem(countSelectItem));
        assertEquals(
                "SELECT c.name, COUNT(c.contributor_id) AS my_count FROM MetaModelSchema.role c GROUP BY c.name HAVING COUNT(c.contributor_id) > 1 ORDER BY COUNT(c.contributor_id) ASC",
                q.toString());

        DataSet data = getDataContext().executeQuery(q);
        assertTrue(data.next());
        assertEquals("Row[values=[founder, 2]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[developer, 5]]", data.getRow().toString());
        assertFalse(data.next());
    }

    public void testHavingFunctionNotSelected() throws Exception {
        Query q = new Query();
        q.from(table2, "c");
        Column roleColumn = table2.getColumnByName(COLUMN_ROLE_ROLE_NAME);
        Column contributorIdColumn = table2.getColumnByName(COLUMN_ROLE_CONTRIBUTOR_ID);

        q.groupBy(roleColumn);
        SelectItem countSelectItem = new SelectItem(FunctionType.COUNT, contributorIdColumn).setAlias("my_count");
        q.select(new SelectItem(roleColumn));
        q.having(new FilterItem(countSelectItem, OperatorType.GREATER_THAN, 3));
        assertEquals("SELECT c.name FROM MetaModelSchema.role c GROUP BY c.name HAVING COUNT(c.contributor_id) > 3",
                q.toString());

        DataSet data = getDataContext().executeQuery(q);
        assertTrue(data.next());
        assertEquals("Row[values=[developer]]", data.getRow().toString());
        assertFalse(data.next());
        data.close();

        q.getHavingClause().removeItems();
        q.having(new FilterItem(SelectItem.getCountAllItem(), OperatorType.GREATER_THAN, 3));
        assertEquals("SELECT c.name FROM MetaModelSchema.role c GROUP BY c.name HAVING COUNT(*) > 3", q.toString());
        data = getDataContext().executeQuery(q);
        assertTrue(data.next());
        assertEquals("Row[values=[developer]]", data.getRow().toString());
        assertFalse(data.next());
        data.close();
    }

    public void testCompiledQueryParameterInWhereClause() throws Exception {
        DataContext dc = getDataContext();
        QueryParameter param1 = new QueryParameter();
        CompiledQuery compiledQuery =
                dc.query().from(table1).select("name").where(COLUMN_CONTRIBUTOR_COUNTRY).eq(param1).compile();
        try {
            assertEquals(1, compiledQuery.getParameters().size());
            assertSame(param1, compiledQuery.getParameters().get(0));

            DataSet ds = dc.executeQuery(compiledQuery, "denmark");
            try {
                assertTrue(ds.next());
                assertEquals("Row[values=[kasper]]", ds.getRow().toString());
                assertTrue(ds.next());
                assertEquals("Row[values=[asbjorn]]", ds.getRow().toString());
                assertTrue(ds.next());
                assertEquals("Row[values=[jesper]]", ds.getRow().toString());
                assertFalse(ds.next());
            } finally {
                ds.close();
            }

            try {
                ds = dc.executeQuery(compiledQuery, "canada");
                assertTrue(ds.next());
                assertEquals("Row[values=[daniel]]", ds.getRow().toString());
                assertFalse(ds.next());
            } finally {
                ds.close();
            }
        } finally {
            compiledQuery.close();
        }
    }

    public void testCompiledQueryParameterInSubQuery() throws Exception {
        final DataContext dc = getDataContext();

        final QueryParameter param1 = new QueryParameter();
        final Query subQuery =
                dc.query().from(table1).select("name").where(COLUMN_CONTRIBUTOR_COUNTRY).eq(param1).toQuery();

        final FromItem subQueryFromItem = new FromItem(subQuery);
        final Query query = new Query().select(new SelectItem(subQuery.getSelectClause().getItem(0), subQueryFromItem))
                .from(subQueryFromItem);

        final CompiledQuery compiledQuery = dc.compileQuery(query);

        try {
            assertEquals(1, compiledQuery.getParameters().size());
            assertSame(param1, compiledQuery.getParameters().get(0));

            DataSet ds = dc.executeQuery(compiledQuery, "denmark");
            List<Object[]> objectArrays = ds.toObjectArrays();
            assertEquals(3, objectArrays.size());

        } finally {
            compiledQuery.close();
        }
    }

    public void testSelectCount() throws Exception {
        DataContext dc = getDataContext();
        Query q = new Query();
        q.from(table1);
        q.selectCount();

        Row row = MetaModelHelper.executeSingleRowQuery(dc, q);
        assertEquals("6", row.getValue(0).toString());
    }

    public void testSimpleSelect() throws Exception {
        DataContext dc = getDataContext();
        Query q = new Query();
        q.from(table1);
        q.select(table1.getColumns());
        DataSet dataSet = dc.executeQuery(q);
        assertTrue(dataSet.next());
        Row row = dataSet.getRow();
        assertEquals("Row[values=[1, kasper, denmark]]", row.toString());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertTrue(dataSet.next());
        assertFalse(dataSet.next());
    }

    public void testCarthesianProduct() throws Exception {
        DataContext dc = getDataContext();
        Query q = new Query();
        q.from(table1);
        q.from(table2);
        q.select(table1.getColumns());
        q.select(table2.getColumns());
        DataSet data = dc.executeQuery(q);
        assertEquals(table1.getColumnCount() + table2.getColumnCount(), data.getSelectItems().size());
        for (int i = 0; i < 6 * 8; i++) {
            assertTrue(data.next());
            if (i == 0) {
                assertEquals("Row[values=[1, kasper, denmark, 1, 1, founder]]", data.getRow().toString());
            } else if (i == 1) {
                assertEquals("Row[values=[1, kasper, denmark, 1, 1, developer]]", data.getRow().toString());
            }
        }
        assertFalse(data.next());
    }

    public void testJoinAndFirstRow() throws Exception {
        DataSet data;

        DataContext dc = getDataContext();
        Query q = new Query();
        q.from(table1);
        q.from(table2);
        q.select(table1.getColumns());
        q.select(table2.getColumns());
        data = dc.executeQuery(q);
        assertEquals(48, data.toObjectArrays().size());

        q.setFirstRow(3);
        data = dc.executeQuery(q);
        assertEquals(46, data.toObjectArrays().size());
    }

    public void testSimpleWhere() throws Exception {
        DataContext dc = getDataContext();
        Query q = new Query();
        q.from(table1);
        q.select(table1.getColumns());
        SelectItem countrySelectItem =
                q.getSelectClause().getSelectItem(table1.getColumnByName(COLUMN_CONTRIBUTOR_COUNTRY));
        q.where(new FilterItem(countrySelectItem, OperatorType.EQUALS_TO, "denmark"));

        DataSet data = dc.executeQuery(q);
        for (int i = 0; i < 3; i++) {
            assertTrue("Assertion failed at i=" + i, data.next());
        }
        assertFalse(data.next());
    }

    public void testMaxRows() throws Exception {
        DataContext dc = getDataContext();
        Query q = new Query();
        q.from(table1);
        q.select(table1.getColumns());
        q.setMaxRows(3);
        DataSet data1 = dc.executeQuery(q);

        assertTrue(data1.next());
        assertEquals("Row[values=[1, kasper, denmark]]", data1.getRow().toString());
        assertTrue(data1.next());
        assertEquals("Row[values=[2, asbjorn, denmark]]", data1.getRow().toString());
        assertTrue(data1.next());
        assertEquals("Row[values=[3, johny, israel]]", data1.getRow().toString());

        assertFalse(data1.next());
        data1.close();

        q = new Query();
        q.from(table1);
        q.select(table1.getColumns());
        q.setFirstRow(2);
        q.setMaxRows(2);
        DataSet data2 = dc.executeQuery(q);
        assertTrue(data2.next());
        assertEquals("Row[values=[2, asbjorn, denmark]]", data2.getRow().toString());
        assertTrue(data2.next());
        assertEquals("Row[values=[3, johny, israel]]", data2.getRow().toString());

        assertFalse(data2.next());
        data2.close();
    }

    public void testCarthesianProductWithWhere() throws Exception {
        DataContext dc = getDataContext();

        SelectItem s1 = new SelectItem(table1.getColumnByName(COLUMN_CONTRIBUTOR_NAME));
        SelectItem s2 = new SelectItem(table2.getColumnByName(COLUMN_ROLE_ROLE_NAME));
        FromItem f1 = new FromItem(table1);
        FromItem f2 = new FromItem(table2);

        Query q = new Query();
        q.select(s1);
        q.select(s2);
        q.from(f1);
        q.from(f2);
        SelectItem s3 = new SelectItem(table1.getColumnByName(COLUMN_CONTRIBUTOR_CONTRIBUTOR_ID));
        SelectItem s4 = new SelectItem(table2.getColumnByName(COLUMN_ROLE_CONTRIBUTOR_ID));
        q.where(new FilterItem(s3, OperatorType.EQUALS_TO, s4));
        assertEquals(
                "SELECT contributor.name, role.name FROM MetaModelSchema.contributor, MetaModelSchema.role WHERE contributor.contributor_id = role.contributor_id",
                q.toString());

        DataSet data = dc.executeQuery(q);
        assertEquals(2, data.getSelectItems().size());
        assertTrue(data.next());
        assertEquals("Row[values=[kasper, founder]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[kasper, developer]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[kasper, developer]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[asbjorn, developer]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[asbjorn, developer]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[daniel, advisor]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[sasidhar, developer]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[jesper, founder]]", data.getRow().toString());
        assertFalse(data.next());
    }

    public void testSelectDistinct() throws Exception {
        // there will be three distinct values in bar column: hello (x2), hi,
        // howdy
        MockDataContext dc = new MockDataContext("sch", "tab", "hello");

        Table table = dc.getTableByQualifiedLabel("sch.tab");
        Query q = dc.query().from(table).select("bar").toQuery();
        q.getSelectClause().setDistinct(true);
        q.orderBy(table.getColumnByName("bar"));

        DataSet ds = dc.executeQuery(q);
        assertTrue(ds.next());
        assertEquals("Row[values=[hello]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[hi]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[yo]]", ds.getRow().toString());
        assertFalse(ds.next());
    }

    public void testSubSelectionAndInnerJoin() throws Exception {
        DataContext dc = getDataContext();

        SelectItem s1 = new SelectItem(table1.getColumnByName(COLUMN_CONTRIBUTOR_NAME));
        SelectItem s2 = new SelectItem(table2.getColumnByName(COLUMN_ROLE_ROLE_NAME));
        FromItem fromItem = new FromItem(JoinType.INNER, table1.getRelationships(table2).iterator().next());

        Query q = new Query();
        q.select(s1);
        q.select(s2);
        q.from(fromItem);
        assertEquals(
                "SELECT contributor.name, role.name FROM MetaModelSchema.contributor INNER JOIN MetaModelSchema.role ON contributor.contributor_id = role.contributor_id",
                q.toString());

        DataSet data = dc.executeQuery(q);
        assertEquals(2, data.getSelectItems().size());
        assertTrue(data.next());
        assertEquals("Row[values=[kasper, founder]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[kasper, developer]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[kasper, developer]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[asbjorn, developer]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[asbjorn, developer]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[daniel, advisor]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[sasidhar, developer]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[jesper, founder]]", data.getRow().toString());
        assertFalse(data.next());
    }

    public void testSubquery() throws Exception {
        Query q1 = new Query();
        q1.from(table1);
        q1.select(table1.getColumns());

        Query q2 = new Query();
        FromItem fromItem = new FromItem(q1);
        q2.from(fromItem);
        SelectItem selectItem = new SelectItem(q1.getSelectClause().getItems().get(1), fromItem);
        selectItem.setAlias("e");
        q2.select(selectItem);
        assertEquals(
                "SELECT name AS e FROM (SELECT contributor.contributor_id, contributor.name, contributor.country FROM MetaModelSchema.contributor)",
                q2.toString());

        fromItem.setAlias("c");
        assertEquals(
                "SELECT c.name AS e FROM (SELECT contributor.contributor_id, contributor.name, contributor.country FROM MetaModelSchema.contributor) c",
                q2.toString());

        DataContext dc = getDataContext();
        DataSet data = dc.executeQuery(q2);
        assertEquals(1, data.getSelectItems().size());
        assertTrue(data.next());
        assertEquals("Row[values=[kasper]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[asbjorn]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[johny]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[daniel]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[sasidhar]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[jesper]]", data.getRow().toString());
        assertFalse(data.next());

        // Create a sub-query for a sub-query
        Query q3 = new Query();
        fromItem = new FromItem(q2);
        q3.from(fromItem);
        selectItem = new SelectItem(q2.getSelectClause().getItems().get(0), fromItem);
        selectItem.setAlias("f");
        q3.select(selectItem);
        fromItem.setAlias("d");
        assertEquals(
                "SELECT d.e AS f FROM (SELECT c.name AS e FROM (SELECT contributor.contributor_id, contributor.name, contributor.country FROM MetaModelSchema.contributor) c) d",
                q3.toString());
        data = dc.executeQuery(q3);
        assertEquals(1, data.getSelectItems().size());
        assertTrue(data.next());
        assertEquals("Row[values=[kasper]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[asbjorn]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[johny]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[daniel]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[sasidhar]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[jesper]]", data.getRow().toString());
        assertFalse(data.next());
    }

    public void testOrderBy() throws Exception {
        Query q = new Query();
        q.from(new FromItem(table1).setAlias("c"));
        q.select(table1.getColumns());
        OrderByItem countryOrderBy = new OrderByItem(q.getSelectClause().getItem(2), Direction.DESC);
        OrderByItem nameOrderBy = new OrderByItem(q.getSelectClause().getItem(1));
        q.orderBy(countryOrderBy, nameOrderBy);

        assertEquals(
                "SELECT c.contributor_id, c.name, c.country FROM MetaModelSchema.contributor c ORDER BY c.country DESC, c.name ASC",
                q.toString());

        DataSet data = getDataContext().executeQuery(q);
        assertTrue(data.next());
        assertEquals("Row[values=[5, sasidhar, unknown]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[3, johny, israel]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[2, asbjorn, denmark]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[6, jesper, denmark]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[1, kasper, denmark]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[4, daniel, canada]]", data.getRow().toString());
        assertFalse(data.next());
    }

    public void testExecuteCount() throws Exception {
        QueryPostprocessDataContext dc = new QueryPostprocessDataContext() {
            @Override
            protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
                throw new UnsupportedOperationException();
            }

            @Override
            protected Number executeCountQuery(Table table, List<FilterItem> whereItems,
                    boolean functionApproximationAllowed) {
                return 1337;
            }

            @Override
            protected String getMainSchemaName() throws MetaModelException {
                return "sch";
            }

            @Override
            protected MutableSchema getMainSchema() throws MetaModelException {
                MutableSchema schema = new MutableSchema(getMainSchemaName());
                MutableTable table = new MutableTable("tabl").setSchema(schema);
                return schema.addTable(table.addColumn(new MutableColumn("col").setTable(table)));
            }
        };

        DataSet ds = dc.query().from("sch.tabl").selectCount().execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1337]]", ds.getRow().toString());
        assertFalse(ds.next());
    }

    public void testExecutePrimaryKeyLookupQuery() throws Exception {
        QueryPostprocessDataContext dc = new QueryPostprocessDataContext() {
            @Override
            protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
                throw new UnsupportedAddressTypeException();
            }

            @Override
            protected Number executeCountQuery(Table table, List<FilterItem> whereItems,
                    boolean functionApproximationAllowed) {
                return null;
            }

            @Override
            protected String getMainSchemaName() throws MetaModelException {
                return "sch";
            }

            @Override
            protected Row executePrimaryKeyLookupQuery(Table table, List<SelectItem> selectItems,
                    Column primaryKeyColumn, Object keyValue) {
                assertEquals("foo", keyValue);
                return new DefaultRow(new SimpleDataSetHeader(selectItems), new Object[] { "hello world" });
            }

            @Override
            protected MutableSchema getMainSchema() throws MetaModelException {
                MutableSchema schema = new MutableSchema(getMainSchemaName());
                MutableTable table = new MutableTable("tabl").setSchema(schema);
                table.addColumn(new MutableColumn("col1").setTable(table).setPrimaryKey(true));
                table.addColumn(new MutableColumn("col2").setTable(table));
                return schema.addTable(table);
            }
        };

        DataSet result = dc.query().from("tabl").select("col2").where("col1").eq("foo").execute();
        assertTrue(result.next());
        assertEquals("Row[values=[hello world]]", result.getRow().toString());
        assertFalse(result.next());
    }

    public void testQueryWithDotInTableName() throws Exception {
        MockDataContext dc = new MockDataContext("folder", "file.csv", "foo");

        Table table = dc.getDefaultSchema().getTableByName("file.csv");
        assertNotNull(table);

        Query q = dc.parseQuery("SELECT foo FROM file.csv WHERE \r\nfoo='bar'");
        assertNotNull(q);

        FilterItem item = q.getWhereClause().getItem(0);
        assertNull(item.getExpression());

        assertEquals("file.csv.foo = 'bar'", item.toSql());
    }

    public void testQueryWithMultipleColumnsInExpression() {
        Query query1 = new Query().from(table1).select("contributor_id,name");
        DataSet set = getDataContext().executeQuery(query1);
        assertEquals(true, set.next());
        assertEquals("Row[values=[1, kasper]]", set.getRow().toString());
        Query query2 = new Query().from(table1).select("Greatest(1,2,3),max(contributer_id)", true);
        assertEquals("SELECT Greatest(1,2,3), MAX(contributer_id) FROM MetaModelSchema.contributor", query2.toString());
        Query query3 = new Query().from(table1).select("*,count(*)");
        assertEquals("SELECT contributor.contributor_id, contributor.name, contributor.country, COUNT(*)"
                + " FROM MetaModelSchema.contributor", query3.toString());
    }

    public void testOrderOnAggregationValue() throws Exception {
        MockDataContext dc = new MockDataContext("sch", "tab", "hello");

        Query query = dc.parseQuery("SELECT MAX(baz) AS X FROM tab GROUP BY baz ORDER BY X");

        DataSet ds = dc.executeQuery(query);

        List<String> values = new ArrayList<String>();

        while (ds.next()) {
            final String value = (String) ds.getRow().getValue(0);
            values.add(value);
        }

        ds.close();

        // this should be alphabetically sorted
        assertEquals("[hello, world]", values.toString());
    }

    public void testColumnOnlyUsedInScalarFunctionInWhereClause() throws Exception {
        final DataContext dc = getDataContext();
        final Query query = dc.parseQuery(
                "SELECT contributor_id FROM contributor WHERE JAVA_SUBSTRING(name, 3, 6) = 'per' ORDER BY contributor_id");
        try (DataSet ds = dc.executeQuery(query)) {
            assertTrue(ds.next());
            // kasper
            assertEquals("1", ds.getRow().getValue(0).toString());
            assertTrue(ds.next());
            // jesper
            assertEquals("6", ds.getRow().getValue(0).toString());
            assertFalse(ds.next());
        }
    }

    public void testQueryDifferentScalarFunctionsOnSameColumnInBothSelectAndWhere() throws Exception {
        final DataContext dc = getDataContext();
        final Query query = dc.parseQuery(
                "SELECT SUBSTRING(name, 1, 3) FROM contributor WHERE JAVA_SUBSTRING(name, 3, 6) = 'per' ORDER BY contributor_id");

        // assert on the parsed select items just to ensure that nothing gets mangled in the parsing
        assertSame(FunctionType.SUBSTRING, query.getSelectClause().getItem(0).getScalarFunction());
        assertEquals(" 1", query.getSelectClause().getItem(0).getFunctionParameters()[0]);
        assertEquals(" 3", query.getSelectClause().getItem(0).getFunctionParameters()[1]);
        assertSame(FunctionType.JAVA_SUBSTRING, query.getWhereClause().getItem(0).getSelectItem().getScalarFunction());
        assertEquals(" 3", query.getWhereClause().getItem(0).getSelectItem().getFunctionParameters()[0]);
        assertEquals(" 6", query.getWhereClause().getItem(0).getSelectItem().getFunctionParameters()[1]);

        try (DataSet ds = dc.executeQuery(query)) {
            assertTrue(ds.next());
            // name is "kasper"
            final Object value1 = ds.getRow().getValue(0);
            assertEquals("kas", value1.toString());
            assertTrue(ds.next());
            // name is "jesper"
            final Object value2 = ds.getRow().getValue(0);
            assertEquals("jes", value2.toString());
            assertFalse(ds.next());
        }
    }
    
    public void testQuerySameScalarFunctionOnSameColumnButDifferentParamsInBothSelectAndWhere() throws Exception {
        final DataContext dc = getDataContext();
        final Query query = dc.parseQuery(
                "SELECT SUBSTRING(name, 1, 3) FROM contributor WHERE SUBSTRING(name, 4, 3) = 'per' ORDER BY contributor_id");

        // assert on the parsed select items just to ensure that nothing gets mangled in the parsing
        assertSame(FunctionType.SUBSTRING, query.getSelectClause().getItem(0).getScalarFunction());
        assertEquals(" 1", query.getSelectClause().getItem(0).getFunctionParameters()[0]);
        assertEquals(" 3", query.getSelectClause().getItem(0).getFunctionParameters()[1]);
        assertSame(FunctionType.SUBSTRING, query.getWhereClause().getItem(0).getSelectItem().getScalarFunction());
        assertEquals(" 4", query.getWhereClause().getItem(0).getSelectItem().getFunctionParameters()[0]);
        assertEquals(" 3", query.getWhereClause().getItem(0).getSelectItem().getFunctionParameters()[1]);

        try (DataSet ds = dc.executeQuery(query)) {
            assertTrue(ds.next());
            // name is "kasper"
            final Object value1 = ds.getRow().getValue(0);
            assertEquals("kas", value1.toString());
            assertTrue(ds.next());
            // name is "jesper"
            final Object value2 = ds.getRow().getValue(0);
            assertEquals("jes", value2.toString());
            assertFalse(ds.next());
        }
    }
}