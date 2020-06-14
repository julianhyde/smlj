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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;

/** Algebraic type. */
public class DataType extends ParameterizedType {
  private final Key key;
  public SortedMap<String, Type> typeConstructors; // TODO make final

  /** Creates a DataType.
   *
   * <p>Called only from {@link TypeSystem#dataTypes(List)}.
   *
   * <p>If the {@code typeSystem} argument is specified, canonizes the types
   * inside type-constructors. This also allows temporary types (necessary while
   * creating self-referential data types) to be replaced with real DataType
   * instances.
   *
   * <p>During replacement, if a type matches {@code placeholderType} it is
   * replaced with {@code this}. This allows cyclic graphs to be copied. */
  DataType(String name, Key key,
      List<? extends Type> parameterTypes,
      SortedMap<String, Type> typeConstructors) {
    super(Op.DATA_TYPE, name, computeMoniker(name, parameterTypes),
        parameterTypes);
    this.key = key;
    this.typeConstructors = Objects.requireNonNull(typeConstructors);
    Preconditions.checkArgument(typeConstructors.comparator() == null
        || typeConstructors.comparator() == Ordering.natural());
  }

  // TODO: move to TS
  static ImmutableSortedMap<String, Type> copyTypeConstructors(
      @Nonnull TypeSystem typeSystem,
      @Nonnull SortedMap<String, Type> typeConstructors,
      @Nonnull UnaryOperator<Type> transform) {
    final ImmutableSortedMap.Builder<String, Type> builder =
        ImmutableSortedMap.naturalOrder();
    typeConstructors.forEach((k, v) ->
        builder.put(k, v.copy(typeSystem, transform)));
    return builder.build();
  }

  public Key key() {
    return key;
  }

  public Keys.DataTypeDef def() {
    return Keys.dataTypeDef(name, parameterTypes, typeConstructors, true);
  }

  public <R> R accept(TypeVisitor<R> typeVisitor) {
    return typeVisitor.visit(this);
  }

  public DataType copy(TypeSystem typeSystem, UnaryOperator<Type> transform) {
    final ImmutableSortedMap<String, Type> typeConstructors =
        copyTypeConstructors(typeSystem, this.typeConstructors, transform);
    return typeConstructors.equals(this.typeConstructors)
        ? this
        : new DataType(name, key, parameterTypes, typeConstructors);
  }

  @Override public Type substitute(TypeSystem typeSystem, List<Type> types,
      TypeSystem.Transaction transaction) {
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
    final List<Keys.DataTypeDef> defs = new ArrayList<>();
    final Map<String, TemporaryType> temporaryTypeMap = new HashMap<>();
    try (TypeSystem.Transaction transaction2 = typeSystem.transaction()) {
      final TypeShuttle typeVisitor = new TypeShuttle(typeSystem) {
        @Override public Type visit(TypeVar typeVar) {
          return types.get(typeVar.ordinal);
        }

        @Override public Type visit(DataType dataType) {
          final String moniker1 = computeMoniker(dataType.name, types);
          final Type type = typeSystem.lookupOpt(moniker1);
          if (type != null) {
            return type;
          }
          final Type type2 = temporaryTypeMap.get(moniker1);
          if (type2 != null) {
            return type2;
          }
          final TemporaryType temporaryType =
              typeSystem.temporaryType(dataType.name, types, transaction2,
                  false);
          temporaryTypeMap.put(moniker1, temporaryType);
          final SortedMap<String, Type> typeConstructors = new TreeMap<>();
          dataType.typeConstructors.forEach((tyConName, tyConType) ->
              typeConstructors.put(tyConName, tyConType.accept(this)));
          defs.add(
              Keys.dataTypeDef(dataType.name, types, typeConstructors, false));
          return temporaryType;
        }
      };
      accept(typeVisitor);
    }
    final List<Type> types1 = typeSystem.dataTypes(defs);
    return types1.get(0);
  }
}

// End DataType.java
