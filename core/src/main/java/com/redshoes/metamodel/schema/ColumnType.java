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
package com.redshoes.metamodel.schema;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Types;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.redshoes.metamodel.annotations.InterfaceStability;
import com.redshoes.metamodel.util.HasName;

/**
 * Represents the data-type of columns.
 */
@InterfaceStability.Stable
public interface ColumnType extends HasName, Serializable {

    /*
     * Literal
     */
    public static final ColumnType CHAR = new ColumnTypeImpl("CHAR", SuperColumnType.LITERAL_TYPE);
    public static final ColumnType VARCHAR = new ColumnTypeImpl("VARCHAR", SuperColumnType.LITERAL_TYPE);
    public static final ColumnType LONGVARCHAR = new ColumnTypeImpl("LONGVARCHAR", SuperColumnType.LITERAL_TYPE);
    public static final ColumnType CLOB = new ColumnTypeImpl("CLOB", SuperColumnType.LITERAL_TYPE, Clob.class, true);
    public static final ColumnType NCHAR = new ColumnTypeImpl("NCHAR", SuperColumnType.LITERAL_TYPE);
    public static final ColumnType NVARCHAR = new ColumnTypeImpl("NVARCHAR", SuperColumnType.LITERAL_TYPE);
    public static final ColumnType LONGNVARCHAR = new ColumnTypeImpl("LONGNVARCHAR", SuperColumnType.LITERAL_TYPE);
    public static final ColumnType NCLOB = new ColumnTypeImpl("NCLOB", SuperColumnType.LITERAL_TYPE, Clob.class, true);

    /*
     * Numbers
     */
    public static final ColumnType TINYINT = new ColumnTypeImpl("TINYINT", SuperColumnType.NUMBER_TYPE, Short.class);
    public static final ColumnType SMALLINT = new ColumnTypeImpl("SMALLINT", SuperColumnType.NUMBER_TYPE, Short.class);
    public static final ColumnType INTEGER = new ColumnTypeImpl("INTEGER", SuperColumnType.NUMBER_TYPE, Integer.class);
    public static final ColumnType BIGINT = new ColumnTypeImpl("BIGINT", SuperColumnType.NUMBER_TYPE, BigInteger.class);
    public static final ColumnType FLOAT = new ColumnTypeImpl("FLOAT", SuperColumnType.NUMBER_TYPE, Double.class);
    public static final ColumnType REAL = new ColumnTypeImpl("REAL", SuperColumnType.NUMBER_TYPE, Double.class);
    public static final ColumnType DOUBLE = new ColumnTypeImpl("DOUBLE", SuperColumnType.NUMBER_TYPE, Double.class);
    public static final ColumnType NUMERIC = new ColumnTypeImpl("NUMERIC", SuperColumnType.NUMBER_TYPE, Double.class);
    public static final ColumnType DECIMAL = new ColumnTypeImpl("DECIMAL", SuperColumnType.NUMBER_TYPE, Double.class);
    public static final ColumnType UUID = new ColumnTypeImpl("UUID", SuperColumnType.NUMBER_TYPE, UUID.class);

    /*
     * Time based
     */
    public static final ColumnType DATE = new ColumnTypeImpl("DATE", SuperColumnType.TIME_TYPE);
    public static final ColumnType TIME = new ColumnTypeImpl("TIME", SuperColumnType.TIME_TYPE);
    public static final ColumnType TIMESTAMP = new ColumnTypeImpl("TIMESTAMP", SuperColumnType.TIME_TYPE);

    /*
     * Booleans
     */
    public static final ColumnType BIT = new ColumnTypeImpl("BIT", SuperColumnType.BOOLEAN_TYPE);
    public static final ColumnType BOOLEAN = new ColumnTypeImpl("BOOLEAN", SuperColumnType.BOOLEAN_TYPE);

    /*
     * Binary types
     */
    public static final ColumnType BINARY = new ColumnTypeImpl("BINARY", SuperColumnType.BINARY_TYPE);
    public static final ColumnType VARBINARY = new ColumnTypeImpl("VARBINARY", SuperColumnType.BINARY_TYPE);
    public static final ColumnType LONGVARBINARY = new ColumnTypeImpl("LONGVARBINARY", SuperColumnType.BINARY_TYPE);
    public static final ColumnType BLOB = new ColumnTypeImpl("BLOB", SuperColumnType.BINARY_TYPE, Blob.class, true);

    /*
     * Other types (as defined in {@link Types}).
     */
    public static final ColumnType NULL = new ColumnTypeImpl("NULL", SuperColumnType.OTHER_TYPE);
    public static final ColumnType OTHER = new ColumnTypeImpl("OTHER", SuperColumnType.OTHER_TYPE);
    public static final ColumnType JAVA_OBJECT = new ColumnTypeImpl("JAVA_OBJECT", SuperColumnType.OTHER_TYPE);
    public static final ColumnType DISTINCT = new ColumnTypeImpl("DISTINCT", SuperColumnType.OTHER_TYPE);
    public static final ColumnType STRUCT = new ColumnTypeImpl("STRUCT", SuperColumnType.OTHER_TYPE);
    public static final ColumnType ARRAY = new ColumnTypeImpl("ARRAY", SuperColumnType.OTHER_TYPE);
    public static final ColumnType REF = new ColumnTypeImpl("REF", SuperColumnType.OTHER_TYPE);
    public static final ColumnType DATALINK = new ColumnTypeImpl("DATALINK", SuperColumnType.OTHER_TYPE);
    public static final ColumnType ROWID = new ColumnTypeImpl("ROWID", SuperColumnType.OTHER_TYPE);
    public static final ColumnType SQLXML = new ColumnTypeImpl("SQLXML", SuperColumnType.OTHER_TYPE);
    public static final ColumnType INET = new ColumnTypeImpl("INET", SuperColumnType.OTHER_TYPE, InetAddress.class);

    /*
     * Additional types (added by MetaModel for non-JDBC datastores)
     */
    public static final ColumnType LIST = new ColumnTypeImpl("LIST", SuperColumnType.OTHER_TYPE, List.class);
    public static final ColumnType MAP = new ColumnTypeImpl("MAP", SuperColumnType.OTHER_TYPE, Map.class);
    public static final ColumnType SET = new ColumnTypeImpl("SET", SuperColumnType.OTHER_TYPE, Set.class);
    public static final ColumnType STRING = new ColumnTypeImpl("STRING", SuperColumnType.LITERAL_TYPE);
    public static final ColumnType NUMBER = new ColumnTypeImpl("NUMBER", SuperColumnType.NUMBER_TYPE);

    public Comparator<Object> getComparator();

    public boolean isBoolean();

    public boolean isBinary();

    public boolean isNumber();

    public boolean isTimeBased();

    public boolean isLiteral();

    public boolean isLargeObject();

    /**
     * @return a java class that is appropriate for handling column values of
     *         this column type
     */
    public Class<?> getJavaEquivalentClass();

    public SuperColumnType getSuperType();

    /**
     * Gets the JDBC type as per the {@link Types} class.
     * 
     * @return an int representing one of the constants in the {@link Types}
     *         class.
     * @throws IllegalStateException
     *             in case getting the JDBC type was unsuccessful.
     */
    public int getJdbcType() throws IllegalStateException;
}