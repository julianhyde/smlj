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
package net.hydromatic.morel.type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;

import net.hydromatic.morel.ast.Op;
import net.hydromatic.morel.util.Ord;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;

/** Algebraic type. */
public class DataType extends BaseType implements NamedType {
  public final String name;
  public final String moniker;
  public final List<? extends Type> parameterTypes;
  public final SortedMap<String, Type> typeConstructors;

  /** Creates a DataType.
   *
   * <p>Called only from {@link TypeSystem#dataType(String, List, Map)}.
   * If the {@code typeSystem} argument is specified, canonizes the types inside
   * type-constructors. This also allows temporary types (necessary while
   * creating self-referential data types) to be replaced with real DataType
   * instances. */
  DataType(TypeSystem typeSystem, String name, String description,
      List<? extends Type> parameterTypes,
      SortedMap<String, Type> typeConstructors) {
    super(Op.DATA_TYPE, description);
    this.name = Objects.requireNonNull(name);
    this.moniker = computeMoniker(name, parameterTypes);
    this.parameterTypes = ImmutableList.copyOf(parameterTypes);
    if (typeSystem == null) {
      this.typeConstructors = ImmutableSortedMap.copyOf(typeConstructors);
    } else {
      this.typeConstructors = copyTypeConstructors(typeSystem, typeConstructors,
          t -> (t instanceof TypeSystem.TemporaryType
                && ((TypeSystem.TemporaryType) t).name().equals(name))
            ? DataType.this
            : t);
    }
    Preconditions.checkArgument(typeConstructors.comparator()
        == Ordering.natural());
  }

  static String computeMoniker(String name, List<? extends Type> typeVars) {
    if (typeVars.isEmpty()) {
      return name;
    }
    final StringBuilder b = new StringBuilder();
    if (typeVars.size() > 1) {
      b.append('(');
    }
    Ord.forEach(typeVars, (t, i) -> {
      if (i > 0) {
        b.append(",");
      }
      if (t instanceof TupleType) {
        b.append('(').append(t.moniker()).append(')');
      } else {
        b.append(t.moniker());
      }
    });
    if (typeVars.size() > 1) {
      b.append(')');
    }
    return b.append(' ').append(name).toString();
  }

  protected ImmutableSortedMap<String, Type> copyTypeConstructors(
      @Nonnull TypeSystem typeSystem,
      @Nonnull SortedMap<String, Type> typeConstructors,
      @Nonnull UnaryOperator<Type> transform) {
    final ImmutableSortedMap.Builder<String, Type> builder =
        ImmutableSortedMap.naturalOrder();
    typeConstructors.forEach((k, v) ->
        builder.put(k, v.copy(typeSystem, transform)));
    return builder.build();
  }

  public <R> R accept(TypeVisitor<R> typeVisitor) {
    return typeVisitor.visit(this);
  }

  public DataType copy(TypeSystem typeSystem, UnaryOperator<Type> transform) {
    final ImmutableSortedMap<String, Type> typeConstructors =
        copyTypeConstructors(typeSystem, this.typeConstructors, transform);
    return typeConstructors.equals(this.typeConstructors)
        ? this
        : new DataType(typeSystem, name, description, parameterTypes,
            typeConstructors);
  }

  public String name() {
    return name;
  }

  @Override public String moniker() {
    return moniker;
  }

  static String computeDescription(Map<String, Type> tyCons) {
    final StringBuilder buf = new StringBuilder();
    tyCons.forEach((tyConName, tyConType) -> {
      if (buf.length() > 1) {
        buf.append(" | ");
      }
      buf.append(tyConName);
      if (tyConType != DummyType.INSTANCE) {
        buf.append(" of ");
        buf.append(tyConType.moniker());
      }
    });
    return buf.toString();
  }
}

// End DataType.java
