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
package net.hydromatic.morel.foreign;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;

import net.hydromatic.morel.type.RecordType;
import net.hydromatic.morel.type.Type;
import net.hydromatic.morel.type.TypeSystem;
import net.hydromatic.morel.util.Ord;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Value based on a Calcite schema.
 *
 * <p>In ML, it appears as a record with a field for each table.
 */
public class CalciteForeignValue implements ForeignValue {
  private final Calcite calcite;
  private final SchemaPlus schema;
  private final boolean lower;

  /** Creates a CalciteForeignValue. */
  public CalciteForeignValue(Calcite calcite, SchemaPlus schema, boolean lower) {
    this.calcite = Objects.requireNonNull(calcite);
    this.schema = Objects.requireNonNull(schema);
    this.lower = lower;
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
    table.getRowType(calcite.typeFactory)
        .getFieldList()
        .forEach(field ->
            fields.put(convert(field.getName()), Converters.fieldType(field)));
    return typeSystem.listType(typeSystem.recordType(fields.build()));
  }

  private String convert(String name) {
    return convert(lower, name);
  }

  private static String convert(boolean lower, String name) {
    return lower ? name.toLowerCase(Locale.ROOT) : name;
  }

  public Object value() {
    final ImmutableList.Builder<List<Object>> fieldValues =
        ImmutableList.builder();
    final List<String> names = Schemas.path(schema).names();
    schema.getTableNames().forEach(tableName -> {
      final RelBuilder b = calcite.relBuilder;
      b.scan(plus(names, tableName));
      final List<RexNode> exprList = b.peek().getRowType()
          .getFieldList().stream()
          .map(f ->
              Ord.of(f.getIndex(),
                  lower ? f.getName().toLowerCase(Locale.ROOT) : f.getName()))
          .sorted(Map.Entry.comparingByValue())
          .map(p -> b.alias(b.field(p.i), p.e))
          .collect(Collectors.toList());
      b.project(exprList, ImmutableList.of(), true);
      final RelNode rel = b.build();
      final Converter converter = Converters.ofRow(rel.getRowType());
      fieldValues.add(new RelList(rel, calcite.dataContext, converter));
    });
    return fieldValues.build();
  }

  /** Returns a copy of a list with one element appended. */
  private static <E> List<E> plus(List<E> list, E e) {
    return ImmutableList.<E>builder().addAll(list).add(e).build();
  }

  /** Converter that creates a record. Uses one sub-Converter per output
   * field. */
  static class RecordConverter implements Converter {
    final Object[] tempValues;
    final Converter[] converters;

    RecordConverter(List<Converter> converterList) {
      tempValues = new Object[converterList.size()];
      converters = converterList.toArray(new Converter[0]);
    }

    @Override public List<Object> apply(Object[] a) {
      for (int i = 0; i < tempValues.length; i++) {
        tempValues[i] = converters[i].apply(a);
      }
      return ImmutableNullableList.copyOf(tempValues);
    }
  }

}

// End CalciteForeignValue.java
