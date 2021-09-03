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
package com.redshoes.metamodel.elasticsearch.rest;

import static com.redshoes.metamodel.elasticsearch.rest.ElasticSearchRestDataContext.DEFAULT_TABLE_NAME;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.swing.table.TableModel;

import com.redshoes.metamodel.create.CreateTable;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.data.DataSetTableModel;
import com.redshoes.metamodel.data.InMemoryDataSet;
import com.redshoes.metamodel.data.Row;
import com.redshoes.metamodel.delete.DeleteFrom;
import com.redshoes.metamodel.elasticsearch.common.ElasticSearchUtils;
import com.redshoes.metamodel.insert.InsertInto;
import com.redshoes.metamodel.query.parser.QueryParserException;
import com.redshoes.metamodel.update.Update;
import org.apache.http.HttpHost;
import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.MetaModelHelper;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.UpdateScript;
import com.redshoes.metamodel.UpdateableDataContext;
import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.FunctionType;
import com.redshoes.metamodel.query.OperatorType;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.MutableColumn;
import com.redshoes.metamodel.schema.MutableTable;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.schema.TableType;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ElasticSearchRestDataContextIT {
    static final int DEFAULT_REST_CLIENT_PORT = 9200;

    private static final String DEFAULT_DOCKER_HOST_NAME = "localhost";

    private static final String INDEX_NAME = "twitter";

    private static RestHighLevelClient client;

    private static UpdateableDataContext dataContext;

    public static String determineHostName() throws URISyntaxException {
        final String dockerHost = System.getenv("DOCKER_HOST");

        if (dockerHost == null) {
            // If no value is returned for the DOCKER_HOST environment variable fall back to a default.
            return DEFAULT_DOCKER_HOST_NAME;
        } else {
            return (new URI(dockerHost)).getHost();
        }
    }

    @Before
    public void setUp() throws Exception {
        final String dockerHostAddress = determineHostName();

        client = ElasticSearchRestUtil
                .createClient(new HttpHost(dockerHostAddress, DEFAULT_REST_CLIENT_PORT), null, null);
        client.indices().create(new CreateIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);

        dataContext = new ElasticSearchRestDataContext(client, INDEX_NAME);
    }

    @After
    public void tearDown() throws IOException {
        client.indices().delete(new DeleteIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
    }

    private static void insertPeopleDocuments() throws IOException {
        indexOnePeopleDocument("female", 20, 5);
        indexOnePeopleDocument("female", 17, 8);
        indexOnePeopleDocument("female", 18, 9);
        indexOnePeopleDocument("female", 19, 10);
        indexOnePeopleDocument("female", 20, 11);
        indexOnePeopleDocument("male", 19, 1);
        indexOnePeopleDocument("male", 17, 2);
        indexOnePeopleDocument("male", 18, 3);
        indexOnePeopleDocument("male", 18, 4);
        indexOnePeopleDocument("", 24, 12);
        indexOnePeopleDocument(null, 25, 13);

        dataContext.refreshSchemas();
    }

    @Test
    public void testNullAndNotNull() throws IOException {
        insertPeopleDocuments();
        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);
        final Column column = table.getColumnByName("gender");

        final FilterItem nullFilterItem = new FilterItem(new SelectItem(column), OperatorType.EQUALS_TO, null);
        final FilterItem notNullFilterItem = new FilterItem(new SelectItem(column), OperatorType.DIFFERENT_FROM, null);

        final Query nullQuery =
                dataContext.query().from(DEFAULT_TABLE_NAME).selectCount().where(nullFilterItem).toQuery();
        final Query notNullQuery =
                dataContext.query().from(DEFAULT_TABLE_NAME).selectCount().where(notNullFilterItem).toQuery();
        final Query allQuery = dataContext.query().from(DEFAULT_TABLE_NAME).selectCount().toQuery();

        final int nullCount =
                ((Number) MetaModelHelper.executeSingleRowQuery(dataContext, nullQuery).getValue(0)).intValue();
        final int notNullCount =
                ((Number) MetaModelHelper.executeSingleRowQuery(dataContext, notNullQuery).getValue(0)).intValue();
        final int allCount =
                ((Number) MetaModelHelper.executeSingleRowQuery(dataContext, allQuery).getValue(0)).intValue();

        assertEquals(nullCount, 1);
        assertEquals(allCount, nullCount + notNullCount);
    }

    @Test
    public void testEmptyAndNotEmpty() throws IOException {
        insertPeopleDocuments();
        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);
        final Column column = table.getColumnByName("gender");

        final FilterItem emptyFilterItem = new FilterItem(new SelectItem(column), OperatorType.EQUALS_TO, "");
        final FilterItem notEmptyFilterItem = new FilterItem(new SelectItem(column), OperatorType.DIFFERENT_FROM, "");

        final Query emptyQuery =
                dataContext.query().from(DEFAULT_TABLE_NAME).selectCount().where(emptyFilterItem).toQuery();
        final Query notEmptyQuery =
                dataContext.query().from(DEFAULT_TABLE_NAME).selectCount().where(notEmptyFilterItem).toQuery();
        final Query allQuery = dataContext.query().from(DEFAULT_TABLE_NAME).selectCount().toQuery();

        final int emptyCount =
                ((Number) MetaModelHelper.executeSingleRowQuery(dataContext, emptyQuery).getValue(0)).intValue();
        final int notEmptyCount =
                ((Number) MetaModelHelper.executeSingleRowQuery(dataContext, notEmptyQuery).getValue(0)).intValue();
        final int allCount =
                ((Number) MetaModelHelper.executeSingleRowQuery(dataContext, allQuery).getValue(0)).intValue();

        assertEquals(emptyCount, 1);
        assertEquals(allCount, emptyCount + notEmptyCount);
    }

    @Test
    public void testSimpleQuery() throws Exception {
        indexTweeterDocument(1);

        Assert.assertArrayEquals(new String[] { DEFAULT_TABLE_NAME }, dataContext
                .getDefaultSchema()
                .getTableNames()
                .toArray());

        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);

        assertThat(table.getColumnNames(), containsInAnyOrder("_id", "message", "postDate", "user"));

        Assert.assertEquals(ColumnType.STRING, table.getColumnByName("user").getType());
        assertEquals(ColumnType.DATE, table.getColumnByName("postDate").getType());
        assertEquals(ColumnType.BIGINT, table.getColumnByName("message").getType());

        dataContext.refreshSchemas();

        try (final DataSet dataSet = dataContext
                .query()
                .from(DEFAULT_TABLE_NAME)
                .select("user")
                .and("message")
                .execute()) {
            assertEquals(ElasticSearchRestDataSet.class, dataSet.getClass());

            assertTrue(dataSet.next());
            assertEquals("Row[values=[user1, 1]]", dataSet.getRow().toString());
        }
    }

    @Test
    public void testDocumentIdAsPrimaryKey() throws Exception {
        indexType2TweeterDocuments();

        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);
        final Column[] primaryKeys = table.getPrimaryKeys().toArray(new Column[0]);
        assertEquals(1, primaryKeys.length);
        assertEquals("_id", primaryKeys[0].getName());

        try (DataSet dataSet = dataContext.query().from(table).select("user", "_id").orderBy("_id").asc().execute()) {
            assertTrue(dataSet.next());
            assertEquals("Row[values=[user1, tweet_tweet2_1]]", dataSet.getRow().toString());
        }
    }

    private void indexType2TweeterDocuments() throws IOException {
        indexTweeterDocument(1);
        indexTweeterDocument(2, null);
        indexTweeterDocument(1);

        dataContext.refreshSchemas();
    }

    @Test
    public void testExecutePrimaryKeyLookupQuery() throws Exception {
        indexType2TweeterDocuments();

        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);
        final Column[] primaryKeys = table.getPrimaryKeys().toArray(new Column[0]);

        try (final DataSet dataSet = dataContext
                .query()
                .from(table)
                .selectAll()
                .where(primaryKeys[0])
                .eq("tweet_tweet2_1")
                .execute()) {
            assertTrue(dataSet.next());
            final Object dateValue = dataSet.getRow().getValue(1);
            assertEquals("Row[values=[tweet_tweet2_1, " + dateValue + ", 1, user1]]", dataSet.getRow().toString());

            assertFalse(dataSet.next());

            assertEquals(InMemoryDataSet.class, dataSet.getClass());
        }
    }

    @Test
    public void testMissingPrimaryKeyLookupQuery() throws Exception {
        indexType2TweeterDocuments();

        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);
        final Column[] primaryKeys = table.getPrimaryKeys().toArray(new Column[0]);

        try (final DataSet dataSet = dataContext
                .query()
                .from(table)
                .selectAll()
                .where(primaryKeys[0])
                .eq("missing")
                .execute()) {
            assertFalse(dataSet.next());
        }
    }

    @Test
    public void testDateIsHandledAsDate() throws Exception {
        indexTweeterDocument(1);

        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);
        final Column column = table.getColumnByName("postDate");
        final ColumnType type = column.getType();
        assertEquals(ColumnType.DATE, type);

        final DataSet dataSet = dataContext.query().from(table).select(column).execute();
        while (dataSet.next()) {
            final Object value = dataSet.getRow().getValue(column);
            assertTrue("Got class: " + value.getClass() + ", expected Date (or subclass)", value instanceof Date);
        }
    }

    @Test
    public void testNumberIsHandledAsNumber() throws Exception {
        insertPeopleDocuments();

        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);
        final Column column = table.getColumnByName("age");
        final ColumnType type = column.getType();
        assertEquals(ColumnType.BIGINT, type);

        final DataSet dataSet = dataContext.query().from(table).select(column).execute();
        while (dataSet.next()) {
            final Object value = dataSet.getRow().getValue(column);
            assertTrue("Got class: " + value.getClass() + ", expected Number (or subclass)", value instanceof Number);
        }
    }

    @Test
    public void testCreateTableAndInsertQuery() throws Exception {
        final Table table = createTable();
        assertNotNull(table);
        Assert.assertEquals("[" + ElasticSearchUtils.FIELD_ID + ", foo, bar]", Arrays
                .toString(table.getColumnNames().toArray()));

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

        try (final DataSet dataSet = dataContext.query().from(table).selectAll().orderBy("bar").execute()) {
            assertTrue(dataSet.next());
            assertEquals("hello", dataSet.getRow().getValue(fooColumn).toString());
            assertNotNull(dataSet.getRow().getValue(idColumn));
            assertTrue(dataSet.next());
            assertEquals("world", dataSet.getRow().getValue(fooColumn).toString());
            assertNotNull(dataSet.getRow().getValue(idColumn));
            assertFalse(dataSet.next());
        }
    }

    @Test
    public void testDeleteAll() throws Exception {
        final Table table = createTable();

        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(table).value("foo", "hello").value("bar", 42).execute();
                callback.insertInto(table).value("foo", "world").value("bar", 43).execute();
            }
        });

        dataContext.executeUpdate(new DeleteFrom(table));

        final Row row = MetaModelHelper
                .executeSingleRowQuery(dataContext, dataContext.query().from(table).selectCount().toQuery());
        assertEquals("Count is wrong", 0, ((Number) row.getValue(0)).intValue());
    }

    private Table createTable() {
        final Schema schema = dataContext.getDefaultSchema();
        final CreateTable createTable = new CreateTable(schema, DEFAULT_TABLE_NAME);
        createTable.withColumn("foo").ofType(ColumnType.STRING);
        createTable.withColumn("bar").ofType(ColumnType.NUMBER);
        dataContext.executeUpdate(createTable);

        return schema.getTableByName(DEFAULT_TABLE_NAME);
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

        final Row row = MetaModelHelper
                .executeSingleRowQuery(dataContext, dataContext.query().from(table).select("foo", "bar").toQuery());
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

        // greater than is not supported
        try {
            dataContext.executeUpdate(new DeleteFrom(table).where("bar").gt(40));
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertEquals("Could not push down WHERE items to delete by query request: [" + DEFAULT_TABLE_NAME
                    + ".bar > 40]", e.getMessage());
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

        final DataSet dataSet = dataContext.query().from(table).select("foo", "bar").orderBy("bar").execute();
        assertTrue(dataSet.next());
        assertEquals("Row[values=[howdy, 42]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[world, 43]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
        dataSet.close();
    }

    @Test
    public void testWhereColumnEqualsValues() throws Exception {
        indexBulkDocuments(INDEX_NAME, 10);

        try (final DataSet dataSet = dataContext
                .query()
                .from(DEFAULT_TABLE_NAME)
                .select("user")
                .and("message")
                .where("user")
                .isEquals("user4")
                .execute()) {
            assertEquals(ElasticSearchRestDataSet.class, dataSet.getClass());

            assertTrue(dataSet.next());
            assertEquals("Row[values=[user4, 4]]", dataSet.getRow().toString());
            assertFalse(dataSet.next());
        }
    }

    @Test
    public void testWhereColumnIsNullValues() throws Exception {
        indexType2TweeterDocuments();

        try (final DataSet dataSet = dataContext
                .query()
                .from(DEFAULT_TABLE_NAME)
                .select("message")
                .where("postDate")
                .isNull()
                .execute()) {
            assertEquals(ElasticSearchRestDataSet.class, dataSet.getClass());

            assertTrue(dataSet.next());
            assertEquals("Row[values=[2]]", dataSet.getRow().toString());
            assertFalse(dataSet.next());
        }
    }

    @Test
    public void testWhereColumnIsNotNullValues() throws Exception {
        indexType2TweeterDocuments();

        try (final DataSet dataSet = dataContext
                .query()
                .from(DEFAULT_TABLE_NAME)
                .select("message")
                .where("postDate")
                .isNotNull()
                .execute()) {
            assertEquals(ElasticSearchRestDataSet.class, dataSet.getClass());

            assertTrue(dataSet.next());
            assertEquals("Row[values=[1]]", dataSet.getRow().toString());
            assertFalse(dataSet.next());
        }
    }

    @Test
    public void testWhereMultiColumnsEqualValues() throws Exception {
        indexBulkDocuments(INDEX_NAME, 10);
        try (final DataSet dataSet = dataContext
                .query()
                .from(DEFAULT_TABLE_NAME)
                .select("user")
                .and("message")
                .where("user")
                .isEquals("user4")
                .and("message")
                .ne(5)
                .execute()) {
            assertEquals(ElasticSearchRestDataSet.class, dataSet.getClass());

            assertTrue(dataSet.next());
            assertEquals("Row[values=[user4, 4]]", dataSet.getRow().toString());
            assertFalse(dataSet.next());
        }
    }

    @Test
    public void testWhereColumnInValues() throws Exception {
        indexBulkDocuments(INDEX_NAME, 10);
        try (final DataSet dataSet = dataContext
                .query()
                .from(DEFAULT_TABLE_NAME)
                .select("user")
                .and("message")
                .where("user")
                .in("user4", "user5")
                .orderBy("message")
                .execute()) {
            assertTrue(dataSet.next());

            final String row1 = dataSet.getRow().toString();
            assertEquals("Row[values=[user4, 4]]", row1);
            assertTrue(dataSet.next());

            final String row2 = dataSet.getRow().toString();
            assertEquals("Row[values=[user5, 5]]", row2);

            assertFalse(dataSet.next());
        }
    }

    @Test
    public void testGroupByQuery() throws Exception {
        insertPeopleDocuments();

        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);

        final Query query = new Query();
        query.from(table);
        final Column genderColumn = table.getColumnByName("gender");
        query.groupBy(genderColumn);
        query
                .select(new SelectItem(genderColumn), new SelectItem(FunctionType.MAX, table
                        .getColumnByName("age")), new SelectItem(FunctionType.MIN, table.getColumnByName("age")),
                        new SelectItem(FunctionType.COUNT, "*", "total"), new SelectItem(FunctionType.MIN, table
                                .getColumnByName("id")).setAlias("firstId"));
        query.where(new FilterItem(new SelectItem(genderColumn), OperatorType.DIFFERENT_FROM, null));
        query.where(new FilterItem(new SelectItem(genderColumn), OperatorType.DIFFERENT_FROM, ""));
        query.orderBy(genderColumn);
        final DataSet data = dataContext.executeQuery(query);
        assertEquals("[" + DEFAULT_TABLE_NAME + ".gender, MAX(" + DEFAULT_TABLE_NAME + ".age), MIN("
                + DEFAULT_TABLE_NAME + ".age), COUNT(*) AS total, MIN(" + DEFAULT_TABLE_NAME + ".id) AS firstId]",
                Arrays.toString(data.getSelectItems().toArray()));

        assertTrue(data.next());
        assertEquals("Row[values=[female, 20, 17, 5, 5]]", data.getRow().toString());
        assertTrue(data.next());
        assertEquals("Row[values=[male, 19, 17, 4, 1]]", data.getRow().toString());
        assertFalse(data.next());
    }

    @Test
    public void testFilterOnNumberColumn() throws Exception {
        indexBulkDocuments(INDEX_NAME, 10);
        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);
        final Query query = dataContext.query().from(table).select("user").where("message").greaterThan(7).toQuery();
        final DataSet data = dataContext.executeQuery(query);
        final String[] expectations = new String[] { "Row[values=[user8]]", "Row[values=[user9]]" };

        assertTrue(data.next());
        assertTrue(Arrays.asList(expectations).contains(data.getRow().toString()));
        assertTrue(data.next());
        assertTrue(Arrays.asList(expectations).contains(data.getRow().toString()));
        assertFalse(data.next());
    }

    @Test
    public void testMaxRows() throws Exception {
        insertPeopleDocuments();

        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);
        final Query query = new Query().from(table).select(table.getColumns()).setMaxRows(5);
        final DataSet dataSet = dataContext.executeQuery(query);

        final TableModel tableModel = new DataSetTableModel(dataSet);
        assertEquals(5, tableModel.getRowCount());
    }

    @Test
    public void testCountQuery() throws Exception {
        indexBulkDocuments(INDEX_NAME, 10);
        final Table table = dataContext.getDefaultSchema().getTableByName(DEFAULT_TABLE_NAME);
        final Query query = new Query().selectCount().from(table);

        final List<Object[]> data = dataContext.executeQuery(query).toObjectArrays();
        assertEquals(1, data.size());
        final Object[] row = data.get(0);
        assertEquals(1, row.length);
        assertEquals(10, ((Number) row[0]).intValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryForANonExistingTable() throws Exception {
        dataContext.query().from("nonExistingTable").select("user").and("message").execute();
    }

    @Test(expected = QueryParserException.class)
    public void testQueryForAnExistingTableAndNonExistingField() throws Exception {
        indexTweeterDocument(1);
        dataContext.query().from(DEFAULT_TABLE_NAME).select("nonExistingField").execute();
    }

    /**
     * Tests that we can explicitly create a table in Elasticsearch. Because Elasticsearch 7.x no longer allows multiple
     * types in one index the table which is created will have the default table name (actually the default
     * Elasticsearch type name), which is "_doc", which is represented the by {@link #DEFAULT_TABLE_NAME} constant.
     */
    @Test
    public void testCreateTable() {
        final CreateTable createTable = new CreateTable(dataContext.getDefaultSchema(), "Dummy");
        createTable.withColumn("foo").ofType(ColumnType.STRING);
        createTable.withColumn("bar").ofType(ColumnType.NUMBER);
        dataContext.executeUpdate(createTable);

        dataContext.refreshSchemas();
        final Schema schema = dataContext.getDefaultSchema();
        assertEquals(1, schema.getTableCount());

        final Table table = schema.getTable(0);
        assertNotNull(table);
        assertEquals(DEFAULT_TABLE_NAME, table.getName());

        assertThat(table.getColumnNames(), containsInAnyOrder(ElasticSearchUtils.FIELD_ID, "foo", "bar"));
    }

    /**
     * Tests that a {@link MetaModelException} is thrown when trying to create a second table, because Elasticsearch 7.x
     * no longer allows multiple types in one index and as a result we can no longer create multiple tables (because a
     * MetaModel table represents an Elasticsearch type).
     */
    @Test(expected = MetaModelException.class)
    public void testCreateSecondTable() {
        final CreateTable createTable = new CreateTable(dataContext.getDefaultSchema(), "FirstTable");
        createTable.withColumn("foo").ofType(ColumnType.STRING);
        dataContext.executeUpdate(createTable);

        dataContext.refreshSchemas();
        final Schema schema = dataContext.getDefaultSchema();
        assertEquals(1, schema.getTableCount());

        final CreateTable createTable2 = new CreateTable(dataContext.getDefaultSchema(), "SecondTable");
        createTable2.withColumn("foo").ofType(ColumnType.STRING);
        dataContext.executeUpdate(createTable);
    }

    @Test
    public void testInsertIntoTable() throws Exception {
        final InsertInto insertInto = new InsertInto(new MutableTable("Whatever", TableType.TABLE, dataContext
                .getDefaultSchema(), new MutableColumn("foo", ColumnType.STRING))).value("foo", "bar");
        dataContext.executeUpdate(insertInto);

        Assert.assertEquals(1, dataContext.query().from(DEFAULT_TABLE_NAME).select("foo").execute().toRows().size());
    }

    @Test(expected = MetaModelException.class)
    public void testInsertIntoSecondTable() throws Exception {
        indexTweeterDocument(1);

        final InsertInto insertInto = new InsertInto(new MutableTable("SecondTable", TableType.TABLE, dataContext
                .getDefaultSchema(), new MutableColumn("foo", ColumnType.STRING))).value("foo", "bar");
        dataContext.executeUpdate(insertInto);
    }

    @Test
    public void testEmptyIndex() throws Exception {
        Assert.assertEquals(0, dataContext.getSchemaByName(INDEX_NAME).getTables().size());
    }

    private static void indexBulkDocuments(final String indexName, final int numberOfDocuments) throws IOException {
        final BulkRequest bulkRequest = new BulkRequest();

        for (int i = 0; i < numberOfDocuments; i++) {
            final IndexRequest indexRequest = new IndexRequest(indexName).id(Integer.toString(i));
            indexRequest.source(buildTweeterJson(i));

            bulkRequest.add(indexRequest);
        }

        client.bulk(bulkRequest, RequestOptions.DEFAULT);

        dataContext.refreshSchemas();
    }

    private static void indexTweeterDocument(final int id, final Date date) throws IOException {
        final IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("tweet_tweet2_" + id);
        indexRequest.source(buildTweeterJson(id, date));

        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    private static void indexTweeterDocument(int id) throws IOException {
        final IndexRequest indexRequest = new IndexRequest(INDEX_NAME).id("tweet_tweet2_" + id);
        indexRequest.source(buildTweeterJson(id));

        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    private static void indexOnePeopleDocument(String gender, int age, int id) throws IOException {
        final IndexRequest indexRequest = new IndexRequest(INDEX_NAME);
        indexRequest.source(buildPeopleJson(gender, age, id));

        client.index(indexRequest, RequestOptions.DEFAULT);
    }

    private static Map<String, Object> buildTweeterJson(int elementId) {
        return buildTweeterJson(elementId, new Date());
    }

    private static Map<String, Object> buildTweeterJson(int elementId, Date date) {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("user", "user" + elementId);
        map.put("postDate", date);
        map.put("message", elementId);
        return map;
    }

    private static XContentBuilder buildPeopleJson(String gender, int age, int elementId) throws IOException {
        return jsonBuilder().startObject().field("gender", gender).field("age", age).field("id", elementId).endObject();
    }
}
