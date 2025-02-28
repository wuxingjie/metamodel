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
package com.redshoes.metamodel.jdbc;

import java.sql.SQLException;

import com.redshoes.metamodel.MetaModelException;
import com.redshoes.metamodel.UpdateScript;

/**
 * MetaModel's representation of {@link SQLException} that gets thrown out of an {@link UpdateScript} and gets
 * converted into a {@link RolledBackUpdateException}.
 */
public class UncheckedSQLException extends MetaModelException {

    private static final long serialVersionUID = 1L;

    public UncheckedSQLException(SQLException e) {
        super(e);
    }

    @Override
    public synchronized SQLException getCause() {
        return (SQLException) super.getCause();
    }
}
