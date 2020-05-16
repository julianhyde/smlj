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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;

import net.hydromatic.morel.ast.Op;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;

/** Algebraic type. */
public class DataType extends ParameterizedType {
  public final SortedMap<String, Type> typeConstructors;

  /** Creates a DataType.
   *
   * <p>Called only from {@link TypeSystem#dataType(String, List, Map, Type)}.
   *
   * <p>If the {@code typeSystem} argument is specified, canonizes the types
   * inside type-constructors. This also allows temporary types (necessary while
   * creating self-referential data types) to be replaced with real DataType
   * instances.
   *
   * <p>During replacement, if a type matches {@code placeholderType} it is
   * replaced with {@code this}. This allows cyclic graphs to be copied. */
  DataType(TypeSystem typeSystem, String name, String description,
      List<? extends Type> parameterTypes,
      SortedMap<String, Type> typeConstructors, Type placeholderType) {
    super(Op.DATA_TYPE, name, computeMoniker(name, parameterTypes),
        description, parameterTypes);
    if (typeSystem == null) {
      this.typeConstructors = ImmutableSortedMap.copyOf(typeConstructors);
    } else {
      this.typeConstructors = copyTypeConstructors(typeSystem, typeConstructors,
          t -> t == placeholderType ? DataType.this : t);
    }
    Preconditions.checkArgument(typeConstructors.comparator() == null
        || typeConstructors.comparator() == Ordering.natural());
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
            typeConstructors, this);
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

  @Override public Type substitute(TypeSystem typeSystem, List<Type> types) {
    // Create a copy of this datatype with type variables substituted with
    // actual types.
    if (types.equals(parameterTypes)) {
      return this;
    }
    final String moniker = computeMoniker(name, types);
    final Type lookup = typeSystem.lookupOpt(moniker);
    if (lookup != null) {
      return lookup;
    }
    assert types.size() == parameterTypes.size();
    final TemporaryType temporaryType =
        typeSystem.temporaryType(name, types, false);
    final TypeShuttle typeVisitor = new TypeShuttle(typeSystem) {
      @Override public Type visit(TypeVar typeVar) {
        return types.get(typeVar.ordinal);
      }

      @Override public Type visit(DataType dataType) {
        return dataType == DataType.this ? temporaryType
            : super.visit(dataType);
      }
    };
    final SortedMap<String, Type> typeConstructors = new TreeMap<>();
    this.typeConstructors.forEach((tyConName, tyConType) ->
        typeConstructors.put(tyConName,
            tyConType.accept(typeVisitor)));
    temporaryType.unregister(typeSystem);
    return typeSystem.dataType(name, types, typeConstructors, temporaryType);
  }
}

// End DataType.java
