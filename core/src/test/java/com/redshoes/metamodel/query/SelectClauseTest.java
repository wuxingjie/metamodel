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
package com.redshoes.metamodel.query;

import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.schema.MutableColumn;
import com.redshoes.metamodel.schema.MutableTable;
import com.redshoes.metamodel.schema.Table;

import junit.framework.TestCase;

public class SelectClauseTest extends TestCase {

	public void testDistinctAddition() throws Exception {
		Table table = new MutableTable("foo");
		Column col = new MutableColumn("bar").setTable(table);

		Query q = new Query();
		q.selectDistinct();
		q.from(table);
		q.select(col);

		assertEquals("SELECT DISTINCT foo.bar FROM foo", q.toSql());
	}
}
