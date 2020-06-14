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

import net.hydromatic.morel.ast.Op;
import net.hydromatic.morel.util.Ord;

import java.util.List;
import java.util.Objects;
import java.util.SortedMap;

/** Type keys. */
public class Keys {
  private Keys() {}

  /** Returns a key that identifies types by name. */
  public static Type.Key name(String name) {
    return new NameKey(name);
  }

  /** Returns a key that identifies types (especially
   * {@link TypeVar type variables}) by ordinal. */
  public static Type.Key ordinal(int ordinal) {
    return new OrdinalKey(ordinal);
  }

  /** Returns a key that identifies an {@link ApplyType}, with an array of
   * types. */
  public static Type.Key apply(ParameterizedType type, Type... argTypes) {
    return apply(type, ImmutableList.copyOf(argTypes));
  }

  /** Returns a key that identifies an {@link ApplyType}. */
  public static Type.Key apply(ParameterizedType type,
      Iterable<? extends Type> argTypes) {
    return new ApplyKey(type, ImmutableList.copyOf(argTypes));
  }

  /** Returns a key that identifies a {@link RecordType}
   * (or a {@link TupleType} if the field names are ascending integers,
   * or {@link PrimitiveType#UNIT unit} if the fields are empty). */
  public static Type.Key record(SortedMap<String, Type> argNameTypes) {
    return new RecordKey(ImmutableSortedMap.copyOfSorted(argNameTypes));
  }

  /** Returns a key that identifies a {@link TupleType}. */
  public static Type.Key tuple(List<? extends Type> argTypes) {
    return new RecordKey(TupleType.toArgNameTypes(argTypes));
  }

  /** Returns a key that identifies a {@link FnType}. */
  public static Type.Key fn(Type paramType, Type resultType) {
    return new OpKey(Op.FN, ImmutableList.of(paramType, resultType));
  }

  /** Returns a key that identifies a {@link ListType}. */
  public static Type.Key list(Type elementType) {
    return new OpKey(Op.LIST, ImmutableList.of(elementType));
  }

  /** Returns a key that identifies a {@link ForallType}. */
  public static Type.Key forall(Type type, List<TypeVar> parameterTypes) {
    return new ForallKey(type, ImmutableList.copyOf(parameterTypes));
  }

  /** Returns a key that identifies a {@link DataType}. */
  public static Type.Key datatype(String name) {
    return new DataTypeKey(name);
  }

  /** Returns a definition of a {@link DataType}. */
  public static DataTypeDef dataTypeDef(String name,
      List<? extends Type> parameterTypes, SortedMap<String, Type> tyCons,
      boolean scheme) {
    return new DataTypeDef(name, parameterTypes, tyCons, scheme);
  }

  /** Key that identifies a type by name. */
  private static class NameKey implements Type.Key {
    private final String name;

    NameKey(String name) {
      this.name = Objects.requireNonNull(name);
    }

    @Override public String toString() {
      return name;
    }

    @Override public int hashCode() {
      return name.hashCode();
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof NameKey
          && ((NameKey) obj).name.equals(name);
    }

    public Type toType() {
      throw new UnsupportedOperationException();
    }
  }

  /** Key that identifies a type by ordinal. */
  private static class OrdinalKey implements Type.Key {
    final int ordinal;

    OrdinalKey(int ordinal) {
      this.ordinal = ordinal;
    }

    @Override public String toString() {
      return TypeVar.name(ordinal);
    }

    @Override public int hashCode() {
      return ordinal;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof OrdinalKey
          && ((OrdinalKey) obj).ordinal == ordinal;
    }

    public Type toType() {
      return new TypeVar(ordinal);
    }
  }

  /** Key of a type that applies a parameterized type to specific type
   * arguments. */
  private static class ApplyKey implements Type.Key {
    final ParameterizedType type;
    final ImmutableList<Type> argTypes;

    ApplyKey(ParameterizedType type, ImmutableList<Type> argTypes) {
      this.type = Objects.requireNonNull(type);
      this.argTypes = Objects.requireNonNull(argTypes);
    }

    @Override public String toString() {
      final StringBuilder b = new StringBuilder();
      switch (argTypes.size()) {
      case 0:
        break;
      case 1:
        b.append(argTypes.get(0).moniker()).append(' ');
        break;
      default:
        b.append('(');
        Ord.forEach(argTypes, (t, i) -> {
          if (i > 0) {
            b.append(',');
          }
          b.append(t.moniker());
        });
        b.append(") ");
      }
      b.append(type.name);
      return b.toString();
    }

    @Override public int hashCode() {
      return Objects.hash(type, argTypes);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof ApplyKey
          && ((ApplyKey) obj).type.equals(type)
          && ((ApplyKey) obj).argTypes.equals(argTypes);
    }

    public Type toType() {
      return new ApplyType(type, argTypes);
    }
  }

  /** Key of a type that applies a built-in type constructor to specific type
   * arguments. */
  private static class OpKey implements Type.Key {
    final Op op;
    final ImmutableList<Type> argTypes;

    OpKey(Op op, ImmutableList<Type> argTypes) {
      this.op = Objects.requireNonNull(op);
      this.argTypes = Objects.requireNonNull(argTypes);
    }

    @Override public String toString() {
      switch (op) {
      case LIST:
        return TypeSystem.unparse(new StringBuilder(), argTypes.get(0), 0,
            Op.LIST.right).append(" list").toString();
      default:
        return TypeSystem.unparseList(new StringBuilder(), op, 0, 0, argTypes)
            .toString();
      }
    }

    @Override public int hashCode() {
      return Objects.hash(op, argTypes);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof OpKey
          && ((OpKey) obj).op.equals(op)
          && ((OpKey) obj).argTypes.equals(argTypes);
    }

    public Type toType() {
      switch (op) {
      case FN:
        assert argTypes.size() == 2;
        return new FnType(argTypes.get(0), argTypes.get(1));
      case LIST:
        assert argTypes.size() == 1;
        return new ListType(argTypes.get(0));
      default:
        throw new AssertionError(op);
      }
    }
  }

  /** Key of a forall type. */
  private static class ForallKey implements Type.Key {
    final Type type;
    final ImmutableList<TypeVar> parameterTypes;

    ForallKey(Type type, ImmutableList<TypeVar> parameterTypes) {
      this.type = Objects.requireNonNull(type);
      this.parameterTypes = Objects.requireNonNull(parameterTypes);
    }

    @Override public String toString() {
      final StringBuilder b = new StringBuilder();
      b.append("forall");
      for (TypeVar type : parameterTypes) {
        b.append(' ').append(type.moniker());
      }
      b.append(". ");
      TypeSystem.unparse(b, type, 0, 0);
      return b.toString();
    }

    @Override public int hashCode() {
      return Objects.hash(type, parameterTypes);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof ForallKey
          && ((ForallKey) obj).type.equals(type)
          && ((ForallKey) obj).parameterTypes.equals(parameterTypes);
    }

    public Type toType() {
      return new ForallType(parameterTypes, type);
    }
  }

  /** Key of a record type. */
  private static class RecordKey implements Type.Key {
    final ImmutableSortedMap<String, Type> argNameTypes;

    RecordKey(ImmutableSortedMap<String, Type> argNameTypes) {
      this.argNameTypes = Objects.requireNonNull(argNameTypes);
      Preconditions.checkArgument(argNameTypes.comparator()
          == RecordType.ORDERING);
    }

    @Override public String toString() {
      switch (argNameTypes.size()) {
      case 0:
        return "()";
      default:
        if (TypeSystem.areContiguousIntegers(argNameTypes.keySet())) {
          return TypeSystem.unparseList(new StringBuilder(), Op.TIMES, 0, 0,
              argNameTypes.values()).toString();
        }
        // fall through
      case 1:
        final StringBuilder builder = new StringBuilder("{");
        argNameTypes.forEach((name, type) -> {
          if (builder.length() > 1) {
            builder.append(", ");
          }
          builder.append(name).append(':').append(type.moniker());
        });
        return builder.append('}').toString();
      }
    }

    @Override public int hashCode() {
      return argNameTypes.hashCode();
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof RecordKey
          && ((RecordKey) obj).argNameTypes.equals(argNameTypes);
    }

    public Type toType() {
      switch (argNameTypes.size()) {
      case 0:
        return PrimitiveType.UNIT;
      default:
        if (TypeSystem.areContiguousIntegers(argNameTypes.keySet())) {
          return new TupleType(ImmutableList.copyOf(argNameTypes.values()));
        }
        // fall through
      case 1:
        return new RecordType(argNameTypes);
      }
    }
  }

  /** Key that identifies a {@code datatype} scheme.
   *
   * <p>See also {@link DataTypeDef}, which has enough information to actually
   * create it. */
  private static class DataTypeKey implements Type.Key {
    final String name;

    DataTypeKey(String name) {
      this.name = name;
    }

    public Type toType() {
      return null;
    }
  }

  /** Information from which a data type can be created. */
  public static class DataTypeDef implements Type.Def {
    final String name;
    final List<? extends Type> types;
    final SortedMap<String, Type> tyCons;
    final boolean scheme;

    private DataTypeDef(String name, List<? extends Type> types,
        SortedMap<String, Type> tyCons, boolean scheme) {
      this.name = Objects.requireNonNull(name);
      this.types = ImmutableList.copyOf(types);
      this.tyCons = ImmutableSortedMap.copyOfSorted(tyCons);
      this.scheme = scheme;
    }

    public StringBuilder describe(StringBuilder buf) {
      final int initialSize = buf.length();
      tyCons.forEach((tyConName, tyConType) -> {
        if (buf.length() > initialSize) {
          buf.append(" | ");
        }
        buf.append(tyConName);
        if (tyConType != DummyType.INSTANCE) {
          buf.append(" of ");
          buf.append(tyConType.moniker());
        }
      });
      return buf;
    }

    public DataType toType(TypeSystem typeSystem) {
      return typeSystem.dataType(name, datatype(name), types, tyCons);
    }
  }
}

// End Keys.java
