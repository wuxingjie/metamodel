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
package com.redshoes.metamodel.csv;

import junit.framework.TestCase;

public class CsvConfigurationTest extends TestCase {

	public void testToString() throws Exception {
		CsvConfiguration conf = new CsvConfiguration(0, "UTF8", ',', '"', '\\',
				true);
		assertEquals(
				"CsvConfiguration[columnNameLineNumber=0, encoding=UTF8, separatorChar=,, quoteChar=\", escapeChar=\\, failOnInconsistentRowLength=true]",
				conf.toString());
	}

	public void testEquals() throws Exception {
		CsvConfiguration conf1 = new CsvConfiguration(0, "UTF8", ',', '"',
				'\\', true);
		CsvConfiguration conf2 = new CsvConfiguration(0, "UTF8", ',', '"',
				'\\', true);

		assertEquals(conf1, conf2);

		CsvConfiguration conf3 = new CsvConfiguration(1, "UTF8", ',', '"',
				'\\', true);
		assertFalse(conf1.equals(conf3));
	}

}