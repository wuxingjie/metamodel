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

import java.util.function.Supplier;

/**
 * Simple/hard implementation of the {@link Ref} interface.
 * 
 * @param <E>
 */
public final class ImmutableRef<E> implements Supplier<E> {

    private final E _object;

    public ImmutableRef(E object) {
        _object = object;
    }

    @Override
    public E get() {
        return _object;
    }

    public static <E> Supplier<E> of(E object) {
        return new ImmutableRef<E>(object);
    }

}
