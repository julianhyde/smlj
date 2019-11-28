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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import com.google.common.collect.ImmutableSortedMap;

import net.hydromatic.sml.type.PrimitiveType;
import net.hydromatic.sml.type.RecordType;
import net.hydromatic.sml.type.Type;
import net.hydromatic.sml.type.TypeSystem;

import java.util.Locale;
import java.util.Objects;

/** Value based on a Calcite schema.
 *
 * <p>In ML, it appears as a record with a field for each table.
 */
class CalciteForeignValue implements ForeignValue {
  private final SqlTypeFactoryImpl typeFactory =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private Schema schema;
  private boolean lower;

  /** Creates a CalciteForeignValue. */
  CalciteForeignValue(Schema schema, boolean lower) {
    this.schema = Objects.requireNonNull(schema);
    this.lower = lower;
  }

  public Type type(TypeSystem typeSystem) {
    final ImmutableSortedMap.Builder<String, Type> fields =
        ImmutableSortedMap.orderedBy(RecordType.ORDERING);
    schema.getTableNames().forEach(tableName -> {
      fields.put(convert(tableName),
          toType(schema.getTable(tableName), typeSystem));
    });
    return typeSystem.recordType(fields.build());
  }

  private Type toType(Table table, TypeSystem typeSystem) {
    final ImmutableSortedMap.Builder<String, Type> fields =
        ImmutableSortedMap.orderedBy(RecordType.ORDERING);
    table.getRowType(typeFactory).getFieldList().forEach(field ->
        fields.put(convert(field.getName()),
            toType(field.getType(), typeSystem)));
    return typeSystem.recordType(fields.build());
  }

  private String convert(String name) {
    return lower ? name.toLowerCase(Locale.ROOT) : name;
  }

  private Type toType(RelDataType type, TypeSystem typeSystem) {
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
      return PrimitiveType.BOOL;
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
      return PrimitiveType.INT;
    case FLOAT:
    case REAL:
    case DOUBLE:
      return PrimitiveType.REAL;
    case VARCHAR:
    case CHAR:
    default:
      return PrimitiveType.STRING;
    }
  }
}

// End CalciteForeignValue.java
