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
package com.redshoes.metamodel.query.builder;

import com.redshoes.metamodel.schema.Column;
import com.redshoes.metamodel.query.FunctionType;
import com.redshoes.metamodel.query.SelectItem;

/**
 * Represents a built query that has a GROUP BY clause.
 */
public interface GroupedQueryBuilder extends
		SatisfiedQueryBuilder<GroupedQueryBuilder> {

	public HavingBuilder having(FunctionType functionType, Column column);
	
	public HavingBuilder having(SelectItem selectItem);
	
	public HavingBuilder having(String columnExpression);

	public SatisfiedOrderByBuilder<GroupedQueryBuilder> orderBy(
			FunctionType function, Column column);
}