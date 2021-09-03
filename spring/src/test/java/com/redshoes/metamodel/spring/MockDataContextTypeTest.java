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
package com.redshoes.metamodel.spring;

import static org.junit.Assert.assertEquals;

import com.redshoes.metamodel.DataContext;
import com.redshoes.metamodel.pojo.PojoDataContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:examples/mock-datacontext-type.xml")
public class MockDataContextTypeTest {

    @Autowired
    private DataContext dataContext;

    @Test
    public void testReadDataContext() {
        assertEquals(PojoDataContext.class, dataContext.getClass());

        assertEquals("here's a value from springs XML file", dataContext.getDefaultSchema().getName());
    }
}
