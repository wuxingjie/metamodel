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
package com.redshoes.metamodel.jdbc.dialects;

import com.redshoes.metamodel.jdbc.JdbcDataContext;

/**
 * Query rewriter for Apache Impala
 *
 * Because Impala uses the same metadata store as Hive to record information about table structure and properties，
 * so Impala provides a high degree of compatibility with the Hive Query Language (HiveQL) ;
 * @see <a href="http://impala.apache.org/docs/build/html/topics/impala_langref.html">impala.apache.org</a>
 **/
public class ImpalaQueryRewriter extends Hive2QueryRewriter{

    public ImpalaQueryRewriter(JdbcDataContext dataContext) {
        super(dataContext);
    }
}

