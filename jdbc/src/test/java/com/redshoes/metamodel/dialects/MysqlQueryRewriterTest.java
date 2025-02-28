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

import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.MutableColumn;
import com.redshoes.metamodel.schema.MutableTable;
import junit.framework.TestCase;

import com.redshoes.metamodel.jdbc.dialects.MysqlQueryRewriter;
import com.redshoes.metamodel.query.OperatorType;
import com.redshoes.metamodel.query.Query;

public class MysqlQueryRewriterTest extends TestCase {

	public void testRewriteLimit() throws Exception {
		Query q = new Query().from(new MutableTable("foo"))
				.select(new MutableColumn("bar")).setMaxRows(25).setFirstRow(6);
		String queryString = new MysqlQueryRewriter(null).rewriteQuery(q);
		assertEquals("SELECT bar FROM foo LIMIT 25 OFFSET 5", queryString);
	}

	public void testRewriteFilterOperandQuote() throws Exception {
		MutableColumn col = new MutableColumn("bar");
		Query q = new Query().from(new MutableTable("foo")).select(col)
				.where(col, OperatorType.EQUALS_TO, "M'jellow strain'ger");
		String queryString = new MysqlQueryRewriter(null).rewriteQuery(q);
		assertEquals("SELECT bar FROM foo WHERE bar = 'M\\'jellow strain\\'ger'",
				queryString);
	}
	
	public void testRewriteLiteralColumnTypesWithoutArgs() throws Exception {
	    MysqlQueryRewriter qr = new MysqlQueryRewriter(null);
        assertEquals("TEXT", qr.rewriteColumnType(ColumnType.VARCHAR, null, null));
        assertEquals("TEXT", qr.rewriteColumnType(ColumnType.NVARCHAR, null, null));
        assertEquals("TEXT", qr.rewriteColumnType(ColumnType.STRING, null, null));
        assertEquals("CHAR", qr.rewriteColumnType(ColumnType.CHAR, null, null));
        assertEquals("NCHAR", qr.rewriteColumnType(ColumnType.NCHAR, null, null));
    }
}