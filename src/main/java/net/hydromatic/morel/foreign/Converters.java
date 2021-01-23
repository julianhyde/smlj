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
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import net.hydromatic.morel.eval.Unit;
import net.hydromatic.morel.type.ListType;
import net.hydromatic.morel.type.PrimitiveType;
import net.hydromatic.morel.type.RecordType;
import net.hydromatic.morel.type.Type;
import net.hydromatic.morel.util.Ord;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** Utilities for Converter. */
public class Converters {
  private Converters() {
  }

  public static Converter ofRow(RelDataType rowType) {
    final List<RelDataTypeField> fields = rowType.getFieldList();
    final List<Converter> converters = new ArrayList<>();
    Ord.forEach(fields, (field, i) ->
        converters.add(ofField(field.getType(), i)));
    return new CalciteForeignValue.RecordConverter(converters);
  }

  public static Converter ofRow2(RelDataType rowType, RecordType type) {
    AtomicInteger ordinal = new AtomicInteger();
    return ofRow3(rowType.getFieldList(), ordinal, type);
  }

  static Converter ofRow3(List<RelDataTypeField> fields,
      AtomicInteger ordinal, RecordType type) {
    final List<Converter> converters = new ArrayList<>();
    for (Type fieldType : type.argNameTypes.values()) {
      converters.add(ofField2(fields, ordinal, fieldType));
    }
    return new CalciteForeignValue.RecordConverter(converters);
  }

  public static Converter ofField(RelDataType type, int ordinal) {
    final FieldConverter fieldConverter = FieldConverter.toType(type);
    return values -> fieldConverter.convertFrom(values[ordinal]);
  }

  static Converter ofField2(List<RelDataTypeField> fields,
      AtomicInteger ordinal, Type type) {
    if (type instanceof RecordType) {
      return ofRow3(fields, ordinal, (RecordType) type);
    }
    final int i = ordinal.getAndIncrement();
    return ofField3(fields.get(i), i, type);
  }

  static Converter ofField3(RelDataTypeField field, int ordinal,
      Type type) {
    final FieldConverter fieldConverter =
        FieldConverter.toType(field.getType());
    return values -> fieldConverter.convertFrom(values[ordinal]);
  }

  @SuppressWarnings("unchecked")
  public static Function<Enumerable<Object[]>, List<Object>>
      fromEnumerable(RelNode rel, Type type) {
    final ListType listType = (ListType) type;
    final RelDataType rowType = rel.getRowType();
    final Function<Object[], Object> elementConverter =
        forType(rowType, listType.elementType);
    return iterable ->
        Lists.newArrayList(
            Iterables.transform(iterable, elementConverter::apply));
  }

  public static Function forType(RelDataType fromType, Type type) {
    if (type == PrimitiveType.UNIT) {
      return o -> Unit.INSTANCE;
    }
    if (type instanceof RecordType) {
      return ofRow2(fromType, (RecordType) type);
    }
    if (type instanceof PrimitiveType) {
      RelDataTypeField field =
          Iterables.getOnlyElement(fromType.getFieldList());
      return Converters.ofField(field.getType(), 0);
    }
    if (fromType.isNullable()) {
      return o -> o == null ? BigDecimal.ZERO : o;
    }
    return o -> o;
  }

  public static Type fieldType(RelDataTypeField field) {
    return FieldConverter.toType(field.getType()).mlType;
  }

  public static RelDataType toCalciteType(Type type,
      RelDataTypeFactory typeFactory) {
    return C2m.forMorel(type, typeFactory).calciteType;
  }

  /** Returns a function that converts from Morel objects to an Enumerable
   * over Calcite rows. */
  public static Function<Object, Enumerable<Object[]>> toCalciteEnumerable(
      Type type, RelDataTypeFactory typeFactory) {
    final C2m converter =
        C2m.forMorel(type, typeFactory);
    return converter::toEnumerable;
  }

  /** Converts a field from Calcite to Morel format. */
  enum FieldConverter {
    FROM_BOOLEAN(PrimitiveType.BOOL) {
      public Boolean convertFrom(Object o) {
        return (Boolean) o;
      }
    },
    FROM_INTEGER(PrimitiveType.INT) {
      public Integer convertFrom(Object o) {
        return o == null ? 0 : ((Number) o).intValue();
      }
    },
    FROM_FLOAT(PrimitiveType.REAL) {
      public Float convertFrom(Object o) {
        return o == null ? 0f : ((Number) o).floatValue();
      }
    },
    FROM_DATE(PrimitiveType.STRING) {
      public String convertFrom(Object o) {
        return o == null ? "" : new Date(
            (Integer) o * DateTimeUtils.MILLIS_PER_DAY).toString();
      }
    },
    FROM_TIME(PrimitiveType.STRING) {
      public String convertFrom(Object o) {
        return o == null ? "" : new Time(
            (Integer) o % DateTimeUtils.MILLIS_PER_DAY).toString();
      }
    },
    FROM_TIMESTAMP(PrimitiveType.STRING) {
      public String convertFrom(Object o) {
        return o == null ? "" : new Timestamp((Long) o).toString();
      }
    },
    FROM_STRING(PrimitiveType.STRING) {
      public String convertFrom(Object o) {
        return o == null ? "" : (String) o;
      }
    };

    final Type mlType;

    FieldConverter(Type mlType) {
      this.mlType = mlType;
    }

    /** Given a Calcite row, returns the value of this field in SML format. */
    public abstract Object convertFrom(Object sourceValue);

    static FieldConverter toType(RelDataType type) {
      switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return FROM_BOOLEAN;

      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return FROM_INTEGER;

      case FLOAT:
      case REAL:
      case DOUBLE:
      case DECIMAL:
        return FROM_FLOAT;

      case DATE:
        return FROM_DATE;

      case TIME:
        return FROM_TIME;

      case TIMESTAMP:
        return FROM_TIMESTAMP;

      case VARCHAR:
      case CHAR:
      default:
        return FROM_STRING;
      }
    }
  }

  /** Converter from Calcite types to Morel types. */
  private static class C2m {
    final RelDataType calciteType;
    final Type morelType;

    C2m(RelDataType calciteType, Type morelType) {
      this.calciteType = calciteType;
      this.morelType = morelType;
    }

    /** Creates a converter for a given Morel type, in the process deducing the
     * corresponding Calcite type. */
    static C2m forMorel(Type type,
        RelDataTypeFactory typeFactory) {
      switch (type.op()) {
      case LIST:
        final ListType listType = (ListType) type;
        return new C2m(
            typeFactory.createMultisetType(
                forMorel(listType.elementType, typeFactory).calciteType, -1),
            type);

      case RECORD_TYPE:
        final RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
        final RecordType recordType = (RecordType) type;
        recordType.argNameTypes.forEach((name, argType) ->
            typeBuilder.add(name, forMorel(argType, typeFactory).calciteType));
        return new C2m(typeBuilder.build(), type);

      case ID:
        final PrimitiveType primitiveType = (PrimitiveType) type;
        switch (primitiveType) {
        case BOOL:
          return new C2m(
              typeFactory.createSqlType(SqlTypeName.BOOLEAN), type);
        case INT:
          return new C2m(
              typeFactory.createSqlType(SqlTypeName.INTEGER), type);
        case REAL:
          return new C2m(
              typeFactory.createSqlType(SqlTypeName.REAL), type);
        case CHAR:
          return new C2m(
              typeFactory.createSqlType(SqlTypeName.SMALLINT), type);
        case UNIT:
          return new C2m(
              typeFactory.createSqlType(SqlTypeName.TINYINT), type);
        case STRING:
          return new C2m(
              typeFactory.createSqlType(SqlTypeName.VARCHAR, -1), type);
        default:
          throw new AssertionError("unknown type " + type);
        }
      }
      throw new UnsupportedOperationException("cannot convert type " + type);
    }

    public Enumerable<Object[]> toEnumerable(Object v) {
      @SuppressWarnings("unchecked")
      final Enumerable<Object> enumerable =
          Linq4j.asEnumerable((List<Object>) v);
      switch (morelType.op()) {
      case LIST:
        final ListType listType = (ListType) morelType;
        final C2m c =
            new C2m(calciteType.getComponentType(),
                listType.elementType);
        return EnumerableDefaults.select(enumerable, c::toArray);
      default:
        throw new AssertionError("cannot convert " + morelType);
      }
    }

    private Object[] toArray(Object o) {
      @SuppressWarnings("unchecked")
      List<Object> list = (List<Object>) o;
      return list.toArray();
    }
  }
}

// End Converters.java
