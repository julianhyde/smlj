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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;

import net.hydromatic.sml.type.PrimitiveType;
import net.hydromatic.sml.type.RecordType;
import net.hydromatic.sml.type.Type;
import net.hydromatic.sml.type.TypeSystem;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;

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
                toType(field).mlType));
    return typeSystem.listType(typeSystem.recordType(fields.build()));
  }

  private String convert(String name) {
    return lower ? name.toLowerCase(Locale.ROOT) : name;
  }

  private FieldConverter toType(RelDataTypeField field) {
    final int ordinal = field.getIndex();
    switch (field.getType().getSqlTypeName()) {
    case BOOLEAN:
      return new FieldConverter(PrimitiveType.BOOL, ordinal) {
        public Boolean convertFrom(Object[] sourceValues) {
          return (Boolean) sourceValues[ordinal];
        }
      };
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
      return new FieldConverter(PrimitiveType.INT, ordinal) {
        public Integer convertFrom(Object[] sourceValues) {
          final Number sourceValue = (Number) sourceValues[ordinal];
          return sourceValue == null ? 0 : sourceValue.intValue();
        }
      };
    case FLOAT:
    case REAL:
    case DOUBLE:
    case DECIMAL:
      return new FieldConverter(PrimitiveType.REAL, ordinal) {
        public Double convertFrom(Object[] sourceValues) {
          final Number sourceValue = (Number) sourceValues[ordinal];
          return sourceValues[ordinal] == null ? 0d : sourceValue.doubleValue();
        }
      };
    case DATE:
      return new FieldConverter(PrimitiveType.STRING, ordinal) {
        public String convertFrom(Object[] sourceValues) {
          return ((java.sql.Date) sourceValues[ordinal]).toString();
        }
      };
    case VARCHAR:
    case CHAR:
    default:
      return new FieldConverter(PrimitiveType.STRING, ordinal) {
        public String convertFrom(Object[] sourceValues) {
          return (String) sourceValues[ordinal];
        }
      };
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

  /** Returns a copy of a list with one element appended. */
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
      supplier = Suppliers.memoize(() ->
          new Interpreter(
              new EmptyDataContext(
                  (JavaTypeFactory) relBuilder.getTypeFactory(),
                  rootSchema(schema)), rel)
              .select(new Converter(rel.getRowType()))
              .toList());
    }

    public Object get(int index) {
      return supplier.get().get(index);
    }

    public int size() {
      return supplier.get().size();
    }

    /** Converts from a Calcite row to an SML record.
     *
     * <p>The Calcite row is represented as an array, ordered by field ordinal;
     * the SML record is represented by a list, ordered by field name
     * (lower-case if {@link #lower}). */
    private class Converter implements Function1<Object[], List<Object>> {
      final Object[] tempValues;
      final FieldConverter[] fieldConverters;

      Converter(RelDataType rowType) {
        final List<RelDataTypeField> fields =
            new ArrayList<>(rowType.getFieldList());
        fields.sort(Comparator.comparing(f -> convert(f.getName())));
        tempValues = new Object[fields.size()];
        fieldConverters = new FieldConverter[fields.size()];
        for (int i = 0; i < fieldConverters.length; i++) {
          fieldConverters[i] = toType(fields.get(i));
        }
      }

      public List<Object> apply(Object[] a) {
        for (int i = 0; i < tempValues.length; i++) {
          tempValues[i] = fieldConverters[i].convertFrom(a);
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

  /** Converts a field from Calcite to SML format. */
  private abstract static class FieldConverter {
    final Type mlType;
    final int ordinal;

    FieldConverter(Type mlType, int ordinal) {
      this.mlType = mlType;
      this.ordinal = ordinal;
    }

    /** Given a Calcite row, returns the value of this field in SML format. */
    public abstract Object convertFrom(Object[] sourceValues);
  }
}

// End CalciteForeignValue.java
