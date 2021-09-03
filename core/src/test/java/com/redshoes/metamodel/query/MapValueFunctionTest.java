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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.redshoes.metamodel.data.DefaultRow;
import com.redshoes.metamodel.data.Row;
import com.redshoes.metamodel.data.SimpleDataSetHeader;
import org.junit.Test;

public class MapValueFunctionTest {

    private final ScalarFunction function = FunctionType.MAP_VALUE;

    @Test
    public void testGetValueFromMap() throws Exception {
        final Map<String, Map<String, Object>> value = new HashMap<>();
        final Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("bar", "baz");
        value.put("foo", innerMap);
        final SelectItem operandItem = new SelectItem("foo", "f");
        final Row row = new DefaultRow(new SimpleDataSetHeader(new SelectItem[] { operandItem }),
                new Object[] { value });
        final Object v1 = function.evaluate(row, new Object[] { "foo.bar" }, operandItem);
        assertEquals("baz", v1.toString());
    }

    @Test
    public void testGetValueFromList() throws Exception {
        final List<Map<String, Object>> value = new ArrayList<>();
        final Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("bar", "baz");
        value.add(innerMap);
        final SelectItem operandItem = new SelectItem("foo", "f");
        final Row row = new DefaultRow(new SimpleDataSetHeader(new SelectItem[] { operandItem }),
                new Object[] { value });
        final Object v1 = function.evaluate(row, new Object[] { "[0]bar" }, operandItem);
        assertEquals("baz", v1.toString());
    }

    @Test
    public void testGetValueFromArray() throws Exception {

        final Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("bar", "baz");
        final Object[] value = {innerMap};
        final SelectItem operandItem = new SelectItem("foo", "f");
        final Row row = new DefaultRow(new SimpleDataSetHeader(new SelectItem[] { operandItem }),
                new Object[] { value });
        final Object v1 = function.evaluate(row, new Object[] { "[0]bar" }, operandItem);
        assertEquals("baz", v1.toString());
    }

    @Test
    public void testNotAMap() throws Exception {
        final SelectItem operandItem = new SelectItem("foo", "f");
        final Row row = new DefaultRow(new SimpleDataSetHeader(new SelectItem[] { operandItem }),
                new Object[] { "not a map" });
        final Object v1 = function.evaluate(row, new Object[] { "foo.bar" }, operandItem);
        assertEquals(null, v1);
    }

}
