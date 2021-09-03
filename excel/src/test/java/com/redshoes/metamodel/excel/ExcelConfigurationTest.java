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
package com.redshoes.metamodel.excel;

import junit.framework.TestCase;

public class ExcelConfigurationTest extends TestCase {

    public void testToString() throws Exception {
        final ExcelConfiguration conf = new ExcelConfiguration(1, true, false);
        assertEquals(String
                .format("ExcelConfiguration[columnNameLineNumber=%s, skipEmptyLines=%s, skipEmptyColumns=%s, "
                        + "detectColumnTypes=%s, numbersOfLinesToScan=%s]", ExcelConfiguration.DEFAULT_COLUMN_NAME_LINE,
                        true, false, false, ExcelConfiguration.DEFAULT_NUMBERS_OF_LINES_TO_SCAN), conf.toString());
    }

	public void testEquals() throws Exception {
		ExcelConfiguration conf1 = new ExcelConfiguration(1, true, false);
		ExcelConfiguration conf2 = new ExcelConfiguration(1, true, false);
		ExcelConfiguration conf3 = new ExcelConfiguration(2, true, false);

		assertEquals(conf1, conf2);
		assertFalse(conf1.equals(conf3));
	}
}
