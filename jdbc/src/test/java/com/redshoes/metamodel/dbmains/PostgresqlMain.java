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
package com.redshoes.metamodel.dbmains;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.redshoes.metamodel.schema.ColumnType;
import com.redshoes.metamodel.schema.Schema;
import com.redshoes.metamodel.schema.Table;
import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.UpdateCallback;
import com.redshoes.metamodel.UpdateScript;
import com.redshoes.metamodel.data.DataSet;
import com.redshoes.metamodel.jdbc.JdbcDataContext;
import com.redshoes.metamodel.query.Query;

public class PostgresqlMain {

	private static final String CONNECTION_STRING = "jdbc:postgresql://localhost/dellstore2";
	private static final String USERNAME = "eobjects";
	private static final String PASSWORD = "eobjects";

	/**
	 * @param args
	 */ 
	public static void main(String[] args) {
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection(CONNECTION_STRING,
					USERNAME, PASSWORD);

			JdbcDataContext dc = new JdbcDataContext(connection);
			final Schema schema = dc.getDefaultSchema();
			dc.executeUpdate(new UpdateScript() {
				@Override
				public void run(UpdateCallback cb) {
					Table table = cb.createTable(schema, "my_table")
							.withColumn("id").ofType(ColumnType.INTEGER)
							.ofNativeType("SERIAL").nullable(false)
							.withColumn("person name").ofSize(255)
							.withColumn("age").ofType(ColumnType.INTEGER)
							.execute();

					for (int i = 0; i < 1000000; i++) {
						cb.insertInto(table).value("person name", "John Doe")
								.value("age", i + 10).execute();
					}

				}
			});

			Table table = schema.getTableByName("my_table");
			Query query = dc.query().from(table).selectCount().toQuery();
			DataSet ds = dc.executeQuery(query);
			ds.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (connection != null) {
					connection.createStatement().execute("DROP TABLE my_table");
				}
			} catch (SQLException e) {
				throw new MetaModelException(
						"Failed to execute INSERT statement", e);
			}
		}

	}

}
