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
package com.redshoes.metamodel.util;

import junit.framework.TestCase;

public class EqualsBuilderTest extends TestCase {

	public void testEquals() throws Exception {
		assertTrue(EqualsBuilder.equals(null, null));
		assertTrue(EqualsBuilder.equals("hello", "hello"));
		assertFalse(EqualsBuilder.equals("hello", null));
		assertFalse(EqualsBuilder.equals(null, "hello"));
		assertFalse(EqualsBuilder.equals("world", "hello"));

		MyCloneable o1 = new MyCloneable();
		assertTrue(EqualsBuilder.equals(o1, o1));
		MyCloneable o2 = o1.clone();
		assertFalse(EqualsBuilder.equals(o1, o2));
	}
	
	static final class MyCloneable implements Cloneable {
		@Override
		public boolean equals(Object obj) {
			return false;
		}

		@Override
		public MyCloneable clone() {
			try {
				return (MyCloneable) super.clone();
			} catch (CloneNotSupportedException e) {
				throw new UnsupportedOperationException();
			}
		}
	};
}
