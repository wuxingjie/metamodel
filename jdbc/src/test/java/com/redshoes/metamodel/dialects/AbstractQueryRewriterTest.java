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
package com.redshoes.metamodel.dialects;

import com.redshoes.metamodel.schema.MutableColumn;
import com.redshoes.metamodel.schema.MutableTable;
import junit.framework.TestCase;

import com.redshoes.metamodel.jdbc.dialects.AbstractQueryRewriter;
import com.redshoes.metamodel.jdbc.dialects.DefaultQueryRewriter;
import com.redshoes.metamodel.query.FilterItem;
import com.redshoes.metamodel.query.FromItem;
import com.redshoes.metamodel.query.OperatorType;
import com.redshoes.metamodel.query.Query;
import com.redshoes.metamodel.query.SelectItem;

public class AbstractQueryRewriterTest extends TestCase {

    public void testRewriteQuery() throws Exception {
        Query q = new Query().selectCount().from(new MutableTable("foobar"))
                .where(new MutableColumn("foob"), OperatorType.EQUALS_TO, null).groupBy(new MutableColumn("col1"))
                .having(new FilterItem(new SelectItem(new MutableColumn("col2")), OperatorType.GREATER_THAN, 40))
                .orderBy(new MutableColumn("bla"));
        assertEquals("SELECT COUNT(*) FROM foobar WHERE foob IS NULL GROUP BY col1 HAVING col2 > 40 ORDER BY bla ASC",
                q.toString());

        AbstractQueryRewriter rewriter = new DefaultQueryRewriter(null) {

        };

        assertEquals("SELECT COUNT(*) FROM foobar WHERE foob IS NULL GROUP BY col1 HAVING col2 > 40 ORDER BY bla ASC",
                rewriter.rewriteQuery(q));

        rewriter = new DefaultQueryRewriter(null) {
            @Override
            protected String rewriteFromItem(Query query, FromItem item) {
                return "mytable";
            }
        };

        assertEquals("SELECT COUNT(*) FROM mytable WHERE foob IS NULL GROUP BY col1 HAVING col2 > 40 ORDER BY bla ASC",
                rewriter.rewriteQuery(q));

        q.getSelectClause().setDistinct(true);

        assertEquals(
                "SELECT DISTINCT COUNT(*) FROM mytable WHERE foob IS NULL GROUP BY col1 HAVING col2 > 40 ORDER BY bla ASC",
                rewriter.rewriteQuery(q));
    }
}