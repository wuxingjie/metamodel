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
package com.redshoes.metamodel;

import java.io.File;

import com.redshoes.metamodel.excel.ExcelConfiguration;
import com.redshoes.metamodel.excel.ExcelDataContext;

import junit.framework.TestCase;

public class DataContextFactoryTest extends TestCase {

    public void testCreateExcelDataContext() throws Exception {
        File file = new File("../excel/src/test/resources/xls_people.xls");
        assertTrue(file.exists());

        UpdateableDataContext dc;

        dc = DataContextFactory.createExcelDataContext(file);
        assertNotNull(dc);
        assertTrue(dc instanceof ExcelDataContext);

        dc = DataContextFactory.createExcelDataContext(file,
                new ExcelConfiguration());
        assertNotNull(dc);
        assertTrue(dc instanceof ExcelDataContext);
    }

}
