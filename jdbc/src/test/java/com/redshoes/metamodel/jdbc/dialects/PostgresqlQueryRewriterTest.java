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
package com.redshoes.metamodel.jdbc.dialects;

import static org.junit.Assert.assertEquals;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.query.SelectItem;
import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.MutableColumn;
import com.redshoes.metamodel.schema.MutableSchema;
import com.redshoes.metamodel.schema.MutableTable;
import org.easymock.EasyMock;
import org.junit.Test;

public class PostgresqlQueryRewriterTest {

    @Test
    public void testInsertNullMap() throws SQLException {
        final PreparedStatement statementMock = EasyMock.createMock(PreparedStatement.class);

        final PostgresqlQueryRewriter queryRewriter = new PostgresqlQueryRewriter(null);
        final Column column = new MutableColumn("col").setType(ColumnType.MAP).setNativeType("jsonb");
        final Object value = null;

        // mock behaviour recording
        statementMock.setObject(0, null);

        EasyMock.replay(statementMock);

        queryRewriter.setStatementParameter(statementMock, 0, column, value);

        EasyMock.verify(statementMock);
    }
    
    @Test
    public void testApproximateCountQueryAndBlankSchemaName() {
        final SelectItem selectItem = SelectItem.getCountAllItem();
        selectItem.setFunctionApproximationAllowed(true);
        final Query query = new Query();
        query.select(selectItem);
        query.from(new MutableTable("tbl").setSchema(new MutableSchema("")));
        assertEquals("SELECT APPROXIMATE COUNT(*) FROM tbl", query.toSql());

        final PostgresqlQueryRewriter queryRewriter = new PostgresqlQueryRewriter(null);
        final String sql = queryRewriter.rewriteQuery(query);
        assertEquals("SELECT COUNT(*) FROM tbl", sql);
    }
}
