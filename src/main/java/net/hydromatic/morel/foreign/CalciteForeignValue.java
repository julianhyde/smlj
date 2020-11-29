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

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;

import net.hydromatic.morel.type.PrimitiveType;
import net.hydromatic.morel.type.RecordType;
import net.hydromatic.morel.type.Type;
import net.hydromatic.morel.type.TypeSystem;
import net.hydromatic.morel.util.Ord;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
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
            fields.put(convert(field.getName()), toType(field.getType()).mlType));
    return typeSystem.listType(typeSystem.recordType(fields.build()));
  }

  private String convert(String name) {
    return convert(lower, name);
  }

  private static String convert(boolean lower, String name) {
    return lower ? name.toLowerCase(Locale.ROOT) : name;
  }

  private static FieldConverter toType(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
      return new FieldConverter(PrimitiveType.BOOL) {
        public Boolean convertFrom(Object o) {
          return (Boolean) o;
        }
      };

    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
      return new FieldConverter(PrimitiveType.INT) {
        public Integer convertFrom(Object o) {
          return o == null ? 0 : ((Number) o).intValue();
        }
      };

    case FLOAT:
    case REAL:
    case DOUBLE:
    case DECIMAL:
      return new FieldConverter(PrimitiveType.REAL) {
        public Float convertFrom(Object o) {
          return o == null ? 0f : ((Number) o).floatValue();
        }
      };

    case DATE:
      return new FieldConverter(PrimitiveType.STRING) {
        public String convertFrom(Object o) {
          return o == null ? ""
              : new Date((Integer) o * DateTimeUtils.MILLIS_PER_DAY).toString();
        }
      };

    case TIME:
      return new FieldConverter(PrimitiveType.STRING) {
        public String convertFrom(Object o) {
          return o == null ? ""
              : new Time((Integer) o % DateTimeUtils.MILLIS_PER_DAY).toString();
        }
      };

    case TIMESTAMP:
      return new FieldConverter(PrimitiveType.STRING) {
        public String convertFrom(Object o) {
          return o == null ? ""
              : new Timestamp((Long) o).toString();
        }
      };

    case VARCHAR:
    case CHAR:
    default:
      return new FieldConverter(PrimitiveType.STRING) {
        public String convertFrom(Object o) {
          return o == null ? "" : (String) o;
        }
      };
    }
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
      final Converter converter =
          Converter.createRecord(lower, rel.getRowType().getFieldList());
      fieldValues.add(new RelList(rel, calcite.dataContext, converter));
    });
    return fieldValues.build();
  }

  /** Returns a copy of a list with one element appended. */
  private static <E> List<E> plus(List<E> list, E e) {
    return ImmutableList.<E>builder().addAll(list).add(e).build();
  }

  /** Converts from a Calcite row to an SML record.
   *
   * <p>The Calcite row is represented as an array, ordered by field ordinal;
   * the SML record is represented by a list, ordered by field name
   * (lower-case if {@link #lower}). */
  public static class Converter implements Function<Object[], Object> {
    final Object[] tempValues;
    final FieldConverter[] fieldConverters;

    public Converter(List<RelDataTypeField> fields) {
      tempValues = new Object[fields.size()];
      fieldConverters = new FieldConverter[fields.size()];
      for (int i = 0; i < fieldConverters.length; i++) {
        fieldConverters[i] = toType(fields.get(i).getType());
      }
    }

    public static Converter createRecord(boolean lower, List<RelDataTypeField> fields) {
      final Ordering<RelDataTypeField> ordering =
          Ordering.from(
              Comparator.comparing(lower
                  ? f -> f.getName().toLowerCase(Locale.ROOT)
                  : RelDataTypeField::getName));
      return new Converter(ordering.immutableSortedCopy(fields));
    }

    public static Function<Object[], Object> createField(RelDataType type) {
      final FieldConverter fieldConverter = toType(type);
      return values -> fieldConverter.convertFrom(values[0]);
    }

    public List<Object> apply(Object[] a) {
      for (int i = 0; i < tempValues.length; i++) {
        tempValues[i] = fieldConverters[i].convertFrom(a[i]);
      }
      return ImmutableNullableList.copyOf(tempValues);
    }
  }

  /** Converts a field from Calcite to SML format. */
  private abstract static class FieldConverter {
    final Type mlType;

    FieldConverter(Type mlType) {
      this.mlType = mlType;
    }

    /** Given a Calcite row, returns the value of this field in SML format. */
    public abstract Object convertFrom(Object sourceValue);
  }
}

// End CalciteForeignValue.java
