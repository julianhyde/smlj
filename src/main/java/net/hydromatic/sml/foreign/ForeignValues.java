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
package net.hydromatic.sml.foreign;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;

import com.google.common.base.Suppliers;

import net.hydromatic.scott.data.hsqldb.ScottHsqldb;

import java.util.function.Supplier;
import javax.sql.DataSource;

/** Helpers for {@link ForeignValue}. */
public abstract class ForeignValues {
  private static final Supplier<SchemaPlus> ROOT_SCHEMA =
      Suppliers.memoize(() -> CalciteSchema.createRootSchema(false).plus());

  private static final Supplier<SchemaPlus> SCOTT_SCHEMA =
      Suppliers.memoize(() -> {
        final DataSource dataSource =
            JdbcSchema.dataSource(ScottHsqldb.URI, null, ScottHsqldb.USER,
                ScottHsqldb.PASSWORD);
        return ROOT_SCHEMA.get().add("scott",
            JdbcSchema.create(ROOT_SCHEMA.get(), "scott",
                dataSource, null, "SCOTT"));
      });

  private ForeignValues() {}

  /** Returns a value based on the Scott JDBC database.
   *
   * <p>It is a record with fields for the following tables:
   *
   * <ul>
   *   <dl>
   *     <dt>dept</dt>
   *     <dd>Departments table</dd>
   *     <dt>emp</dt>
   *     <dd>Employees table</dd>
   *   </dl>
   * </ul>
   */
  public static ForeignValue scott() {
    return new CalciteForeignValue(SCOTT_SCHEMA.get(), true);
  }
}

// End ForeignValues.java
