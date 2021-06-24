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
package com.redshoes.metamodel.elasticsearch.nativeclient;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.swing.table.TableModel;

import com.redshoes.metamodel.MetaModelHelper;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.UpdateScript;
import com.redshoes.metamodel.UpdateableDataContext;
import com.redshoes.metamodel.create.CreateTable;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.data.DataSetTableModel;
import com.redshoes.metamodel.data.InMemoryDataSet;
import com.redshoes.metamodel.data.Row;
import com.redshoes.metamodel.delete.DeleteFrom;
import com.redshoes.metamodel.elasticsearch.common.ElasticSearchUtils;
import com.redshoes.metamodel.query.FunctionType;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.query.parser.QueryParserException;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.update.Update;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class ElasticSearchDataContextTest extends ESSingleNodeTestCase {

    private static final String indexName = "twitter";
    private static final String indexType1 = "tweet1";
    private static final String indexType2 = "tweet2";
    private static final String indexName2 = "twitter2";
    private static final String indexType3 = "tweet3";
    private static final String bulkIndexType = "bulktype";
    private static final String peopleIndexType = "peopletype";
    private static final String mapping =
            "{\"date_detection\":\"false\",\"properties\":{\"message\":{\"type\":\"keyword\",\"doc_values\":\"true\"}}}";
    private Client client;
    private UpdateableDataContext dataContext;

    @Before
    public void beforeTests() throws Exception {
        client = client();

        dataContext = new ElasticSearchDataContext(client, indexName);
    }

    private void insertPeopleDocuments() throws IOException {
        indexOnePeopleDocument("female", 20, 5);
        indexOnePeopleDocument("female", 17, 8);
        indexOnePeopleDocument("female", 18, 9);
        indexOnePeopleDocument("female", 19, 10);
        indexOnePeopleDocument("female", 20, 11);
        indexOnePeopleDocument("male", 19, 1);
        indexOnePeopleDocument("male", 17, 2);
        indexOnePeopleDocument("male", 18, 3);
        indexOnePeopleDocument("male", 18, 4);
        dataContext.refreshSchemas();
    }

    @After
    public void afterTests() {
        client.admin().indices().delete(new DeleteIndexRequest("_all")).actionGet();
    }

    @Test
    public void testSimpleQuery() throws Exception {
        indexTweeterDocument(indexType1, 1);
        dataContext.refreshSchemas();

        Table table = dataContext.getDefaultSchema().getTableByName("tweet1");
        try (DataSet ds = dataContext.query().from(indexType1).select("_id").execute()) {
            assertEquals(ElasticSearchDataSet.class, ds.getClass());
            assertTrue(ds.next());
            assertEquals("Row[values=[tweet_tweet1_1]]", ds.getRow().toString());
        }

        assertEquals("[_id, message, postDate, user]", Arrays.toString(table.getColumnNames().toArray()));

        assertEquals(ColumnType.STRING, table.getColumnByName("_id").getType());
        assertEquals(ColumnType.STRING, table.getColumnByName("user").getType());
        assertEquals(ColumnType.DATE, table.getColumnByName("postDate").getType());
        assertEquals(ColumnType.BIGINT, table.getColumnByName("message").getType());

        try (DataSet ds = dataContext.query().from(indexType1).select("user").and("message").execute()) {
            assertEquals(ElasticSearchDataSet.class, ds.getClass());

            assertTrue(ds.next());
            assertEquals("Row[values=[user1, 1]]", ds.getRow().toString());
        }
    }

    @Test
    public void testDocumentIdAsPrimaryKey() throws Exception {
        indexTweeterDocuments();

        Table table = dataContext.getDefaultSchema().getTableByName("tweet2");
        Column[] pks = table.getPrimaryKeys().toArray(new Column[0]);
        assertEquals(1, pks.length);
        assertEquals("_id", pks[0].getName());

        try (DataSet ds = dataContext.query().from(table).select("user", "_id").orderBy("_id").asc().execute()) {
            assertTrue(ds.next());
            assertEquals("Row[values=[user1, tweet_tweet2_1]]", ds.getRow().toString());
        }
    }

    @Test
    public void testExecutePrimaryKeyLookupQuery() throws Exception {
        indexTweeterDocuments();

        Table table = dataContext.getDefaultSchema().getTableByName("tweet2");
        Column[] pks = table.getPrimaryKeys().toArray(new Column[0]);

        try (DataSet ds = dataContext.query().from(table).selectAll().where(pks[0]).eq("tweet_tweet2_1").execute()) {
            assertTrue(ds.next());
            Object dateValue = ds.getRow().getValue(2);
            assertEquals("Row[values=[tweet_tweet2_1, 1, " + dateValue + ", user1]]", ds.getRow().toString());

            assertFalse(ds.next());

            assertEquals(InMemoryDataSet.class, ds.getClass());
        }
    }

    @Test
    public void testDateIsHandledAsDate() throws Exception {
        indexTweeterDocument(indexType1, 1);

        Table table = dataContext.getDefaultSchema().getTableByName("tweet1");
        Column column = table.getColumnByName("postDate");
        ColumnType type = column.getType();
        assertEquals(ColumnType.DATE, type);

        DataSet dataSet = dataContext.query().from(table).select(column).execute();
        while (dataSet.next()) {
            Object value = dataSet.getRow().getValue(column);
            assertTrue("Got class: " + value.getClass() + ", expected Date (or subclass)", value instanceof Date);
        }
    }

    @Test
    public void testNumberIsHandledAsNumber() throws Exception {
        insertPeopleDocuments();

        Table table = dataContext.getDefaultSchema().getTableByName(peopleIndexType);
        Column column = table.getColumnByName("age");
        ColumnType type = column.getType();
        assertEquals(ColumnType.BIGINT, type);

        DataSet dataSet = dataContext.query().from(table).select(column).execute();
        while (dataSet.next()) {
            Object value = dataSet.getRow().getValue(column);
            assertTrue("Got class: " + value.getClass() + ", expected Number (or subclass)", value instanceof Number);
        }
    }

    @Test
    public void testCreateTableAndInsertQuery() throws Exception {
        final Table table = createTable();
        assertEquals("[" + ElasticSearchUtils.FIELD_ID + ", foo, bar]",
                Arrays.toString(table.getColumnNames().toArray()));

        final Column fooColumn = table.getColumnByName("foo");
        final Column idColumn = table.getPrimaryKeys().get(0);
        assertEquals("Column[name=_id,columnNumber=0,type=STRING,nullable=null,nativeType=null,columnSize=null]",
                idColumn.toString());

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
            }
        });

        dataContext.refreshSchemas();

        try (DataSet ds = dataContext.query().from(table).selectAll().orderBy("bar").execute()) {
            assertTrue(ds.next());
            assertEquals("hello", ds.getRow().getValue(fooColumn).toString());
            assertNotNull(ds.getRow().getValue(idColumn));
            assertTrue(ds.next());
            assertEquals("world", ds.getRow().getValue(fooColumn).toString());
            assertNotNull(ds.getRow().getValue(idColumn));
            assertFalse(ds.next());
        }
    }

    @Test
    public void testDeleteFromWithWhere() throws Exception {
        final Table table = createTable();

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
            }
        });

        dataContext.executeUpdate(new DeleteFrom(table).where("bar").eq(42));

        final Row row = MetaModelHelper.executeSingleRowQuery(dataContext,
                dataContext.query().from(table).selectCount().toQuery());

        assertEquals("Row[values=[1]]", row.toString());

    }

    @Test
    public void testDeleteNoWhere() throws Exception {
        final Table table = createTable();

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
            }
        });

        dataContext.executeUpdate(new DeleteFrom(table));

        Row row = MetaModelHelper.executeSingleRowQuery(dataContext,
                dataContext.query().from(table).selectCount().toQuery());
        assertEquals("Row[values=[0]]", row.toString());
    }

    @Test
    public void testDeleteByQuery() throws Exception {
        final Table table = createTable();

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
            }
        });

        dataContext.executeUpdate(new DeleteFrom(table).where("foo").eq("hello").where("bar").eq(42));

        Row row = MetaModelHelper.executeSingleRowQuery(dataContext,
                dataContext.query().from(table).select("foo", "bar").toQuery());
        assertEquals("Row[values=[world, 43]]", row.toString());
    }

    @Test
    public void testDeleteUnsupportedQueryType() throws Exception {
        final Table table = createTable();

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
            }
        });

        // greater than is not yet supported
        try {
            dataContext.executeUpdate(new DeleteFrom(table).where("bar").gt(40));
            fail("Exception expected");
        } catch (UnsupportedOperationException e) {
            final String msg = e.getMessage();
            assertTrue("Got: " + msg, msg.startsWith("Could not push down WHERE items to delete by query request:"));
        }
    }

    @Test
    public void testUpdateRow() throws Exception {
        final Table table = createTable();

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
            }
        });

        dataContext.executeUpdate(new Update(table).value("foo", "howdy").where("bar").eq(42));

        DataSet dataSet = dataContext.query().from(table).select("foo", "bar").orderBy("bar").execute();
        assertTrue(dataSet.next());
        assertEquals("Row[values=[howdy, 42]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[world, 43]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
        dataSet.close();
    }

    @Test
    public void testWhereColumnEqualsValues() throws Exception {
        indexBulkDocuments(indexName, bulkIndexType, 10);

        try (DataSet ds = dataContext.query().from(bulkIndexType).select("user").and("message").where("user")
                .isEquals("user4").execute()) {
            assertEquals(ElasticSearchDataSet.class, ds.getClass());

            assertTrue(ds.next());
            assertEquals("Row[values=[user4, 4]]", ds.getRow().toString());
            assertFalse(ds.next());
        }
    }

    @Test
    public void testWhereColumnIsNullValues() throws Exception {
        indexTweeterDocuments();

        try (DataSet ds = dataContext.query().from(indexType2).select("message").where("postDate").isNull().execute()) {
            assertEquals(ElasticSearchDataSet.class, ds.getClass());

            assertTrue(ds.next());
            assertEquals("Row[values=[2]]", ds.getRow().toString());
            assertFalse(ds.next());
        }
    }

    @Test
    public void testWhereColumnIsNotNullValues() throws Exception {
        indexTweeterDocuments();

        try (DataSet ds =
                dataContext.query().from(indexType2).select("message").where("postDate").isNotNull().execute()) {
            assertEquals(ElasticSearchDataSet.class, ds.getClass());

            assertTrue(ds.next());
            assertEquals("Row[values=[1]]", ds.getRow().toString());
            assertFalse(ds.next());
        }
    }

    @Test
    public void testWhereMultiColumnsEqualValues() throws Exception {
        indexBulkDocuments(indexName, bulkIndexType, 10);

        try (DataSet ds = dataContext.query().from(bulkIndexType).select("user").and("message").where("user")
                .isEquals("user4").and("message").ne(5).execute()) {
            assertEquals(ElasticSearchDataSet.class, ds.getClass());

            assertTrue(ds.next());
            assertEquals("Row[values=[user4, 4]]", ds.getRow().toString());
            assertFalse(ds.next());
        }
    }

    @Test
    public void testWhereColumnInValues() throws Exception {
        indexBulkDocuments(indexName, bulkIndexType, 10);

        try (DataSet ds = dataContext.query().from(bulkIndexType).select("user").and("message").where("user")
                .in("user4", "user5").orderBy("message").execute()) {
            assertTrue(ds.next());

            String row1 = ds.getRow().toString();
            assertEquals("Row[values=[user4, 4]]", row1);
            assertTrue(ds.next());

            String row2 = ds.getRow().toString();
            assertEquals("Row[values=[user5, 5]]", row2);

            assertFalse(ds.next());
        }
    }

    @Test
    public void testGroupByQuery() throws Exception {
        insertPeopleDocuments();

        Table table = dataContext.getDefaultSchema().getTableByName(peopleIndexType);

        Query q = new Query();
        q.from(table);
        q.groupBy(table.getColumnByName("gender"));
        q.select(new SelectItem(table.getColumnByName("gender")),
                new SelectItem(FunctionType.MAX, table.getColumnByName("age")),
                new SelectItem(FunctionType.MIN, table.getColumnByName("age")),
                new SelectItem(FunctionType.COUNT, "*", "total"),
                new SelectItem(FunctionType.MIN, table.getColumnByName("id")).setAlias("firstId"));
        q.orderBy("gender");
        DataSet data = dataContext.executeQuery(q);
        assertEquals(
                "[peopletype.gender, MAX(peopletype.age), MIN(peopletype.age), COUNT(*) AS total, MIN(peopletype.id) AS firstId]",
                Arrays.toString(data.getSelectItems().toArray()));

        assertTrue(data.next());
        assertEquals("Row[values=[female, 20, 17, 5, 5]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[male, 19, 17, 4, 1]]", data.getRow().toString());
        assertFalse(data.next());
    }

    @Test
    public void testFilterOnNumberColumn() {
        indexBulkDocuments(indexName, bulkIndexType, 10);

        Table table = dataContext.getDefaultSchema().getTableByName(bulkIndexType);
        Query q = dataContext.query().from(table).select("user").where("message").greaterThan(7).toQuery();
        DataSet data = dataContext.executeQuery(q);
        String[] expectations = new String[] { "Row[values=[user8]]", "Row[values=[user9]]" };

        assertTrue(data.next());
        assertTrue(Arrays.asList(expectations).contains(data.getRow().toString()));
        assertTrue(data.next());
        assertTrue(Arrays.asList(expectations).contains(data.getRow().toString()));
        assertFalse(data.next());
    }

    @Test
    public void testMaxRows() throws Exception {
        insertPeopleDocuments();

        Table table = dataContext.getDefaultSchema().getTableByName(peopleIndexType);
        Query query = new Query().from(table).select(table.getColumns()).setMaxRows(5);
        DataSet dataSet = dataContext.executeQuery(query);

        TableModel tableModel = new DataSetTableModel(dataSet);
        assertEquals(5, tableModel.getRowCount());
    }

    @Test
    public void testCountQuery() throws Exception {
        indexBulkDocuments(indexName, bulkIndexType, 10);

        Table table = dataContext.getDefaultSchema().getTableByName(bulkIndexType);
        Query q = new Query().selectCount().from(table);

        List<Object[]> data = dataContext.executeQuery(q).toObjectArrays();
        assertEquals(1, data.size());
        Object[] row = data.get(0);
        assertEquals(1, row.length);
        assertEquals("[10]", Arrays.toString(row));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryForANonExistingTable() throws Exception {
        dataContext.query().from("nonExistingTable").select("user").and("message").execute();
    }

    @Test(expected = QueryParserException.class)
    public void testQueryForAnExistingTableAndNonExistingField() throws Exception {
        indexTweeterDocument(indexType1, 1);
        dataContext.query().from(indexType1).select("nonExistingField").execute();
    }

    @Test
    public void testNonDynamicMappingTableNames() throws Exception {
        CreateIndexRequest cir = new CreateIndexRequest(indexName2);
        client.admin().indices().create(cir).actionGet();

        PutMappingRequest pmr = new PutMappingRequest(indexName2).type(indexType3).source(mapping, XContentType.JSON);

        client.admin().indices().putMapping(pmr).actionGet();

        ElasticSearchDataContext dataContext2 = new ElasticSearchDataContext(client, indexName2);

        assertEquals("[tweet3]", Arrays.toString(dataContext2.getDefaultSchema().getTableNames().toArray()));
    }

    private void indexBulkDocuments(String indexName, String indexType, int numberOfDocuments) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        for (int i = 0; i < numberOfDocuments; i++) {
            bulkRequest
                    .add(client.prepareIndex(indexName, indexType, Integer.toString(i)).setSource(buildTweeterJson(i)));
        }
        bulkRequest.execute().actionGet();
        
        dataContext.refreshSchemas();
    }

    private void indexTweeterDocuments() {
        indexTweeterDocument(indexType2, 1);
        indexTweeterDocument(indexType2, 2, null);
        indexTweeterDocument(indexType2, 1);
        
        dataContext.refreshSchemas();
    }

    private void indexTweeterDocument(String indexType, int id, Date date) {
        final String id1 = "tweet_" + indexType + "_" + id;
        client.prepareIndex(indexName, indexType, id1).setSource(buildTweeterJson(id, date)).execute().actionGet();
    }

    private void indexTweeterDocument(String indexType, int id) {
        client.prepareIndex(indexName, indexType).setSource(buildTweeterJson(id)).setId("tweet_" + indexType + "_" + id)
                .execute().actionGet();
    }

    private void indexOnePeopleDocument(String gender, int age, int id) throws IOException {
        client.prepareIndex(indexName, peopleIndexType).setSource(buildPeopleJson(gender, age, id)).execute()
                .actionGet();
    }

    private static Map<String, Object> buildTweeterJson(int elementId) {
        return buildTweeterJson(elementId, new Date());
    }

    private static Map<String, Object> buildTweeterJson(int elementId, Date date) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("user", "user" + elementId);
        map.put("postDate", date);
        map.put("message", elementId);
        return map;
    }

    private static XContentBuilder buildPeopleJson(String gender, int age, int elementId) throws IOException {
        return jsonBuilder().startObject().field("gender", gender).field("age", age).field("id", elementId).endObject();
    }

    private Table createTable() {
        client.admin().indices().prepareCreate(indexName).execute().actionGet();
        final Schema schema = dataContext.getDefaultSchema();
        final CreateTable createTable = new CreateTable(schema, "testCreateTable");
        createTable.withColumn("foo").ofType(ColumnType.STRING);
        createTable.withColumn("bar").ofType(ColumnType.DOUBLE);
        dataContext.executeUpdate(createTable);

        return schema.getTableByName("testCreateTable");
    }
}
