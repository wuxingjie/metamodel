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

import java.util.function.Function;

/**
 * A function that always returns the same constant response.
 * 
 * @param <I>
 * @param <O>
 */
public final class ConstantFunc<I, O> implements Function<I, O> {

    private final O _response;

    public ConstantFunc(O response) {
        _response = response;
    }

    @Override
    public O apply(I arg) {
        return _response;
    }

    @Override
    public int hashCode() {
        if (_response == null) {
            return -1;
        }
        return _response.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof ConstantFunc) {
            Object otherResponse = ((ConstantFunc<?, ?>) obj)._response;
            if (otherResponse == null && _response == null) {
                return true;
            } else if (_response == null) {
                return false;
            } else {
                return _response.equals(otherResponse);
            }
        }
        return false;
    }
}
