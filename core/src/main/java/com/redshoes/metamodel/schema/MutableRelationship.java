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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectInputStream.GetField;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.redshoes.metamodel.util.LegacyDeserializationObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable implementation of the Relationship interface.
 * 
 * The immutability help ensure integrity of object-relationships. To create
 * relationships use the <code>createRelationship</code> method.
 */
public class MutableRelationship extends AbstractRelationship implements
		Serializable, Relationship {

	private static final long serialVersionUID = 238786848828528822L;
	private static final Logger logger = LoggerFactory
			.getLogger(MutableRelationship.class);

	private final List<Column> _primaryColumns;
	private final List<Column> _foreignColumns;

	/**
	 * Factory method to create relations between two tables by specifying which
	 * columns from the tables that enforce the relationship.
	 * 
	 * @param primaryColumns
	 *            the columns from the primary key table
	 * @param foreignColumns
	 *            the columns from the foreign key table
	 * @return the relation created
	 */
	public static Relationship createRelationship(List<Column> primaryColumns,
			List<Column> foreignColumns) {
		Table primaryTable = checkSameTable(primaryColumns);
		Table foreignTable = checkSameTable(foreignColumns);
		MutableRelationship relation = new MutableRelationship(primaryColumns,
				foreignColumns);

		if (primaryTable instanceof MutableTable) {
			try {
				((MutableTable) primaryTable).addRelationship(relation);
			} catch (UnsupportedOperationException e) {
				// this is an allowed behaviour - not all tables need to support
				// this method.
				logger.debug(
						"primary table ({}) threw exception when adding relationship",
						primaryTable);
			}

			// Ticket #144: Some tables have relations with them selves and then
			// the
			// relationship should only be added once.
			if (foreignTable != primaryTable
					&& foreignTable instanceof MutableTable) {
				try {
					((MutableTable) foreignTable).addRelationship(relation);
				} catch (UnsupportedOperationException e) {
					// this is an allowed behaviour - not all tables need to
					// support this method.
					logger.debug(
							"foreign table ({}) threw exception when adding relationship",
							foreignTable);
				}
			}
		}
		return relation;
	}

	public void remove() {
		Table primaryTable = getPrimaryTable();
		if (primaryTable instanceof MutableTable) {
			((MutableTable) primaryTable).removeRelationship(this);
		}
		Table foreignTable = getForeignTable();
		if (foreignTable instanceof MutableTable) {
			((MutableTable) foreignTable).removeRelationship(this);
		}
	}

	public static Relationship createRelationship(Column primaryColumn,
			Column foreignColumn) {
		List<Column> pcols = new ArrayList<>();
		pcols.add(primaryColumn);
		List<Column> fcols = new ArrayList<>();
		fcols.add(foreignColumn);


		return createRelationship(pcols, fcols);
	}

	/**
	 * Prevent external instantiation
	 */
	private MutableRelationship(List<Column> primaryColumns, List<Column> foreignColumns) {
		_primaryColumns = primaryColumns;
		_foreignColumns = foreignColumns;
	}

	@Override
	public List<Column> getPrimaryColumns() {
		return _primaryColumns;
	}

	@Override
	public List<Column> getForeignColumns() {
		return _foreignColumns;
	}

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        final GetField getFields = stream.readFields();
        Object primaryColumns = getFields.get("_primaryColumns", null);
        Object foreignColumns = getFields.get("_foreignColumns", null);
        if (primaryColumns instanceof Column[] && foreignColumns instanceof Column[]) {
            primaryColumns = Arrays.<Column> asList((Column[]) primaryColumns);
            foreignColumns = Arrays.<Column> asList((Column[]) foreignColumns);
        }
        LegacyDeserializationObjectInputStream.setField(MutableRelationship.class, this, "_primaryColumns",
                primaryColumns);
        LegacyDeserializationObjectInputStream.setField(MutableRelationship.class, this, "_foreignColumns",
                foreignColumns);
    }
}