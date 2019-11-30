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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;

import net.hydromatic.sml.type.PrimitiveType;
import net.hydromatic.sml.type.RecordType;
import net.hydromatic.sml.type.Type;
import net.hydromatic.sml.type.TypeSystem;
import net.hydromatic.sml.util.Ord;
import net.hydromatic.sml.util.Pair;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Value based on a Calcite schema.
 *
 * <p>In ML, it appears as a record with a field for each table.
 */
class CalciteForeignValue implements ForeignValue {
  private final SchemaPlus schema;
  private final boolean lower;
  private final RelBuilder relBuilder;

  /** Creates a CalciteForeignValue. */
  CalciteForeignValue(SchemaPlus schema, boolean lower) {
    this.schema = Objects.requireNonNull(schema);
    this.lower = lower;
    this.relBuilder = RelBuilder.create(Frameworks.newConfigBuilder()
        .defaultSchema(rootSchema(schema))
        .build());
  }

  private static SchemaPlus rootSchema(SchemaPlus schema) {
    for (;;) {
      if (schema.getParentSchema() == null) {
        return schema;
      }
      schema = schema.getParentSchema();
    }
  }

  public Type type(TypeSystem typeSystem) {
    final ImmutableSortedMap.Builder<String, Type> fields =
        ImmutableSortedMap.orderedBy(RecordType.ORDERING);
    schema.getTableNames().forEach(tableName ->
        fields.put(convert(tableName),
            toType(schema.getTable(tableName), typeSystem)));
    return typeSystem.recordType(fields.build());
  }

  private Type toType(Table table, TypeSystem typeSystem) {
    final ImmutableSortedMap.Builder<String, Type> fields =
        ImmutableSortedMap.orderedBy(RecordType.ORDERING);
    table.getRowType(relBuilder.getTypeFactory())
        .getFieldList()
        .forEach(field ->
            fields.put(convert(field.getName()),
                toType(field.getType(), typeSystem)));
    return typeSystem.listType(typeSystem.recordType(fields.build()));
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

  public Object value() {
    final ImmutableList.Builder<List<Object>> fieldValues =
        ImmutableList.builder();
    final List<String> names = Schemas.path(schema).names();
    schema.getTableNames().forEach(tableName ->
        fieldValues.add(
            new RelList(relBuilder.scan(plus(names, tableName)).build())));
    return fieldValues.build();
  }

  private static <E> List<E> plus(List<E> list, E e) {
    return ImmutableList.<E>builder().addAll(list).add(e).build();
  }

  /** A list whose contents are computed by evaluating a relational
   * expression. */
  private class RelList extends AbstractList<Object> {
    private final RelNode rel;

    private final Supplier<List<List<Object>>> supplier;

    protected RelList(RelNode rel) {
      this.rel = rel;
      supplier = Suppliers.memoize(() -> new Interpreter(
          new EmptyDataContext(
              (JavaTypeFactory) relBuilder.getTypeFactory(),
              rootSchema(schema)),
          rel)
          .select(new Converter(rel.getRowType()))
          .toList());
    }

    public Object get(int index) {
      return supplier.get().get(index);
    }

    public int size() {
      return supplier.get().size();
    }

    private class Converter implements Function1<Object[], List<Object>> {
      final int[] fields;
      final Object[] tempValues;

      Converter(RelDataType rowType) {
        final List<Ord<String>> fieldNames = new ArrayList<>();
        for (String fieldName : rowType.getFieldNames()) {
          fieldNames.add(Ord.of(fieldNames.size(), convert(fieldName)));
        }
        fieldNames.sort(Comparator.comparing(o -> o.e));
        fields = ImmutableIntList.copyOf(fieldNames.stream().map(o -> o.i)
            .collect(Collectors.toList())).toIntArray();
        tempValues = new Object[fields.length];
      }

      public List<Object> apply(Object[] a) {
        for (int i = 0; i < tempValues.length; i++) {
          tempValues[i] = a[fields[i]];
        }
        return ImmutableNullableList.copyOf(tempValues);
      }
    }
  }

  /** Data context that has no variables. */
  private static class EmptyDataContext implements DataContext {
    private final JavaTypeFactory typeFactory;
    private final SchemaPlus rootSchema;

    EmptyDataContext(JavaTypeFactory typeFactory, SchemaPlus rootSchema) {
      this.typeFactory = typeFactory;
      this.rootSchema = rootSchema;
    }

    public SchemaPlus getRootSchema() {
      return rootSchema;
    }

    public JavaTypeFactory getTypeFactory() {
      return typeFactory;
    }

    public QueryProvider getQueryProvider() {
      throw new UnsupportedOperationException();
    }

    public Object get(String name) {
      return null;
    }
  }
}

// End CalciteForeignValue.java
