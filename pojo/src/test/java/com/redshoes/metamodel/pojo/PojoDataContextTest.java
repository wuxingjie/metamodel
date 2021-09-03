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
package com.redshoes.metamodel.pojo;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.UpdateScript;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.data.Row;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.util.SimpleTableDef;
import junit.framework.TestCase;

import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.LogicalOperator;
import com.redshoes.metamodel.query.OperatorType;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.query.SelectItem;

public class PojoDataContextTest extends TestCase {

    public void testExampleScenario() throws Exception {
        Collection<Person> persons = new ArrayList<Person>();
        persons.add(new Person("Bono", 42));
        persons.add(new Person("Elvis Presley", 42));

        Map<String, Object> record1 = new HashMap<String, Object>();
        record1.put("name", "Bruce Springsteen");
        record1.put("title", "The Boss");

        Map<String, Object> record2 = new HashMap<String, Object>();
        record2.put("name", "Elvis Presley");
        record2.put("title", "The King");

        List<Map<String, ?>> titles = new ArrayList<Map<String, ?>>();
        titles.add(record1);
        titles.add(record2);

        TableDataProvider<?> provider1 = new ObjectTableDataProvider<Person>("persons", Person.class, persons);
        TableDataProvider<?> provider2 = new MapTableDataProvider(new SimpleTableDef("titles", new String[] { "name",
                "title" }), titles);

        DataContext dc = new PojoDataContext(Arrays.<TableDataProvider<?>> asList(provider1, provider2));

        DataSet dataSet = dc.query().from("persons").innerJoin("titles").on("name", "name").selectAll().execute();

        assertEquals("[persons.age, persons.name, titles.name, titles.title]",
                Arrays.toString(dataSet.getSelectItems().toArray()));
        assertTrue(dataSet.next());
        assertEquals("Row[values=[42, Elvis Presley, Elvis Presley, The King]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
    }

    public void testScenarioWithMap() throws Exception {
        final SimpleTableDef tableDef = new SimpleTableDef("bar", new String[] { "col1", "col2", "col3" },
                new ColumnType[] { ColumnType.VARCHAR, ColumnType.INTEGER, ColumnType.BOOLEAN });
        final List<Map<String, ?>> maps = new ArrayList<Map<String, ?>>();
        maps.add(createMap("2", 1000, true));
        maps.add(createMap("1", 1001, false));
        maps.add(createMap("1", 1002, true));
        maps.add(createMap("2", 1003, false));
        maps.add(createMap("2", 1004, false));
        final TableDataProvider<?> tableDataProvider = new MapTableDataProvider(tableDef, maps);
        runScenario(tableDataProvider);
    }

    public void testScenarioWithArrays() throws Exception {
        final SimpleTableDef tableDef = new SimpleTableDef("bar", new String[] { "col1", "col2", "col3" },
                new ColumnType[] { ColumnType.VARCHAR, ColumnType.INTEGER, ColumnType.BOOLEAN });
        final List<Object[]> arrays = new ArrayList<Object[]>();
        arrays.add(new Object[] { "2", 1000, true });
        arrays.add(new Object[] { "1", 1001, false });
        arrays.add(new Object[] { "1", 1002, true });
        arrays.add(new Object[] { "2", 1003, false });
        arrays.add(new Object[] { "2", 1004, false });
        final TableDataProvider<?> tableDataProvider = new ArrayTableDataProvider(tableDef, arrays);
        runScenario(tableDataProvider);
    }

    public void testScenarioWithObjects() throws Exception {
        final Collection<FoobarBean> collection = new ArrayList<FoobarBean>();
        collection.add(new FoobarBean("2", 1000, true));
        collection.add(new FoobarBean("1", 1001, false));
        collection.add(new FoobarBean("1", 1002, true));
        collection.add(new FoobarBean("2", 1003, false));
        collection.add(new FoobarBean("2", 1004, false));
        final TableDataProvider<?> tableDataProvider = new ObjectTableDataProvider<FoobarBean>("bar", FoobarBean.class,
                collection);
        runScenario(tableDataProvider);
    }

    private void runScenario(TableDataProvider<?> tableDataProvider) {
        final PojoDataContext dc = new PojoDataContext("foo", tableDataProvider);

        final Schema schema = dc.getDefaultSchema();
        assertEquals(2, schema.getTableCount());
        final Table table = schema.getTable(0);
        assertEquals("foo.bar", table.getQualifiedLabel());

        assertEquals("[col1, col2, col3]", Arrays.toString(table.getColumnNames().toArray()));

        DataSet ds = dc.query().from("bar").select("col2").where("col3").eq(true).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1000]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[1002]]", ds.getRow().toString());
        assertFalse(ds.next());

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.deleteFrom(table).where("col1").eq("1").execute();
            }
        });

        ds = dc.query().from("bar").select("col1", "col2", "col3").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 1000, true]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 1003, false]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 1004, false]]", ds.getRow().toString());
        assertFalse(ds.next());

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("col1", "3").value("col2", 1005).value("col3", true).execute();
            }
        });

        ds = dc.query().from("bar").select("col1", "col2", "col3").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 1000, true]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 1003, false]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, 1004, false]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[3, 1005, true]]", ds.getRow().toString());
        assertFalse(ds.next());

        Query qOrderd = dc.query().from("bar").select("col1").toQuery().selectDistinct().orderBy("col1");
        ds = dc.executeQuery(qOrderd);
        assertTrue(ds.next());
        assertEquals("Row[values=[2]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[3]]", ds.getRow().toString());
        assertFalse(ds.next());

        Query qUnordered = dc.query().from("bar").select("col1").toQuery().selectDistinct();
        ds = dc.executeQuery(qUnordered);
        assertTrue(ds.next());
        //Check both possibilities for the order, because not for certain
        if(ds.getRow().toString().equals("Row[values=[2]]")){
            assertEquals("Row[values=[2]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[3]]", ds.getRow().toString());
        }else{
            assertEquals("Row[values=[3]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[2]]", ds.getRow().toString());
        }
        assertFalse(ds.next());

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.dropTable(table).execute();
            }
        });

        assertEquals(0, schema.getTableCount());

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.createTable(schema, "yo!").withColumn("foo").withColumn("bar").execute();
                callback.insertInto("yo!").value("foo", "1").value("bar", "2").execute();
            }
        });

        assertEquals(2, schema.getTableCount());

        ds = dc.query().from("yo!").select("foo", "bar").execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1, 2]]", ds.getRow().toString());
        assertFalse(ds.next());
    }

    private Map<String, ?> createMap(Object a, Object b, Object c) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("col1", a);
        map.put("col2", b);
        map.put("col3", c);
        return map;
    }

    public void testJoinWithCompoundFilterItems() {
        final List<Role> roles = new ArrayList<>();
        roles.add(new Role(1, "admin"));
        roles.add(new Role(2, "guest"));

        final List<User> users = new ArrayList<>();
        users.add(new User("pete", null, 1));
        users.add(new User("jake", Date.valueOf("2018-1-1"), 1));
        users.add(new User("susan", Date.valueOf("2020-1-1"), 2));

        final DataContext dataContext = new PojoDataContext("userdb", new ObjectTableDataProvider<User>("users",
                User.class, users), new ObjectTableDataProvider<Role>("roles", Role.class, roles));

        final SelectItem expirationDateField = new SelectItem(dataContext
                .getColumnByQualifiedLabel("userdb.users.expirationDate"));
        final Column userRoleIdColumn = dataContext.getColumnByQualifiedLabel("userdb.users.roleId");
        final Column roleIdColumn = dataContext.getColumnByQualifiedLabel("userdb.roles.id");

        DataSet dataSet = dataContext
                .query()
                .from("users")
                .and("roles")
                .select("users.name")
                .where(userRoleIdColumn)
                .eq(roleIdColumn)
                .where(new FilterItem(LogicalOperator.OR, new FilterItem(expirationDateField, OperatorType.EQUALS_TO,
                        null), new FilterItem(expirationDateField, OperatorType.GREATER_THAN_OR_EQUAL, Date
                                .valueOf("2019-1-1"))))
                .orderBy("name")
                .execute();

        List<Row> rows = dataSet.toRows();
        assertEquals(2, rows.size());
        assertEquals("pete", rows.get(0).getValue(0));
        assertEquals("susan", rows.get(1).getValue(0));
    }
}