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

import com.google.common.collect.ImmutableList;

import net.hydromatic.morel.ast.Op;
import net.hydromatic.morel.util.Ord;

import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

/** Type that is a polymorphic type applied to a set of types. */
public class ApplyType extends BaseType {
  public final Type type;
  public final ImmutableList<Type> types;

  protected ApplyType(Type type, ImmutableList<Type> types,
      String description) {
    super(Op.APPLY_TYPE, description);
    this.type = Objects.requireNonNull(type);
    this.types = Objects.requireNonNull(types);
  }

  static String computeDescription(Type type, List<Type> types) {
    final StringBuilder b = new StringBuilder();
    switch (types.size()) {
    case 0:
      break;
    case 1:
      b.append(types.get(0).moniker()).append(' ');
      break;
    default:
      b.append('(');
      Ord.forEach(types, (t, i) -> {
        if (i > 0) {
          b.append(',');
        }
        b.append(t.moniker());
      });
      b.append(") ");
    }
    b.append(type.moniker());
    return b.toString();
  }

  public <R> R accept(TypeVisitor<R> typeVisitor) {
    return typeVisitor.visit(this);
  }

  public Type copy(TypeSystem typeSystem, UnaryOperator<Type> transform) {
    final Type type2 = type.copy(typeSystem, transform);
    //noinspection UnstableApiUsage
    final ImmutableList<Type> types2 =
        types.stream().map(t -> t.copy(typeSystem, transform))
            .collect(ImmutableList.toImmutableList());
    return type == type2 && types.equals(types2) ? this
        : typeSystem.apply(type2, types2);
  }
}

// End ApplyType.java
