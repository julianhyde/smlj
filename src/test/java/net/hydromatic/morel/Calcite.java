/*
 * Licensed to Julian Hyde under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership.
 * Julian Hyde licenses this file to you under the Apache
 * License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the
 * License.
 */
package net.hydromatic.morel;

import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableMap;

import net.hydromatic.morel.foreign.ForeignValue;

import java.util.Map;

/** Runtime context. */
class Calcite {
  final RelBuilder relBuilder;
  final ImmutableMap<String, ForeignValue> valueMap;

  Calcite(Map<String, DataSet> dataSetMap) {
    relBuilder = RelBuilder.create(Frameworks.newConfigBuilder().build());
    final ImmutableMap.Builder<String, ForeignValue> b = ImmutableMap.builder();
    dataSetMap.forEach((name, dataSet) -> b.put(
        name,
        dataSet.foreignValue(relBuilder)));
    this.valueMap = b.build();
  }
}

// End Calcite.java
