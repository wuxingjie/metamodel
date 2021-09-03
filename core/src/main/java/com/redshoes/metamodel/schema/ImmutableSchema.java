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
package com.redshoes.metamodel.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * An immutable implementation of the {@link Schema} interface.
 */
public final class ImmutableSchema extends AbstractSchema implements
		Serializable {

	private static final long serialVersionUID = 1L;

	private final List<ImmutableTable> tables = new ArrayList<ImmutableTable>();
	private String name;
	private String quote;

	private ImmutableSchema(String name, String quote) {
		super();
		this.name = name;
		this.quote = quote;
	}

	public ImmutableSchema(Schema schema) {
		this(schema.getName(), schema.getQuote());
		List<Table> origTables = schema.getTables();
		for (Table table : origTables) {
			tables.add(new ImmutableTable(table, this));
		}

		Collection<Relationship> origRelationships = schema.getRelationships();
		for (Relationship relationship : origRelationships) {
			ImmutableRelationship.create(relationship, this);
		}
	}

	@Override
	public List<Table> getTables() {
		return Collections.unmodifiableList(tables);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getQuote() {
		return quote;
	}
}
