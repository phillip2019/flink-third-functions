/*
 * ====================================================================
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
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 * ============================= NOTE =================================
 * This code has been copied from
 * https://github.com/apache/httpcomponents-client/tree/rel/v4.5.13
 * and it was changed to use in this project.
 * ====================================================================
 */

package com.chinagoods.bigdata.connectors.http.internal.utils.uri;

import lombok.Data;
import org.apache.flink.util.Preconditions;

@Data
public class NameValuePair {

    private final String name;

    private final String value;

    /**
     * Default Constructor taking a name and a value. The value may be null.
     *
     * @param name The name.
     * @param value The value.
     */
    public NameValuePair(final String name, final String value) {
        super();
        this.name = Preconditions.checkNotNull(name, "Name may not be null");
        this.value = value;
    }
}
