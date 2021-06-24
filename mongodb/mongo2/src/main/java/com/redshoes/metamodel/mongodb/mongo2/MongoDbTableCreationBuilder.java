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
package com.redshoes.metamodel.mongodb.mongo2;

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.create.AbstractTableCreationBuilder;
import com.redshoes.metamodel.create.TableCreationBuilder;
import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.ImmutableColumn;
import com.redshoes.metamodel.schema.MutableTable;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;

final class MongoDbTableCreationBuilder extends
        AbstractTableCreationBuilder<MongoDbUpdateCallback> implements
        TableCreationBuilder {

	public MongoDbTableCreationBuilder(MongoDbUpdateCallback updateCallback,
                                       Schema schema, String name) {
		super(updateCallback, schema, name);
	}

	@Override
	public Table execute() throws MetaModelException {
		final MongoDbDataContext dataContext = getUpdateCallback()
				.getDataContext();
		final Schema schema = dataContext.getDefaultSchema();
		final MutableTable table = getTable();
		if (table.getColumnByName("_id") == null) {
			// all mongo db collections have an _id field as the first field.
			ImmutableColumn idColumn = new ImmutableColumn("_id",
					ColumnType.ROWID, table, table.getColumnCount(), null,
					null, null, null, true, null, true);
			table.addColumn(idColumn);
		}
		table.setSchema(schema);
		getUpdateCallback().createCollection(table.getName());
		dataContext.addTable(table);
		return table;
	}
}
