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

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

import com.google.common.collect.ImmutableMap;

import net.hydromatic.morel.ast.Ast;
import net.hydromatic.morel.compile.Compiler;
import net.hydromatic.morel.compile.Compiles;
import net.hydromatic.morel.compile.Environment;
import net.hydromatic.morel.compile.Environments;
import net.hydromatic.morel.compile.TypeResolver;
import net.hydromatic.morel.eval.Code;
import net.hydromatic.morel.eval.Codes;
import net.hydromatic.morel.eval.EvalEnv;
import net.hydromatic.morel.parse.MorelParserImpl;
import net.hydromatic.morel.parse.ParseException;
import net.hydromatic.morel.type.Type;
import net.hydromatic.morel.type.TypeSystem;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Calcite table-valued user-defined function that evaluates a Morel
 * expression and returns the result as a relation. */
public class CalciteMorelTableFunction {
  private CalciteMorelTableFunction() {
  }

  public static final SqlOperator OPERATOR =
      new SqlUserDefinedTableFunction(
          new SqlIdentifier("morel", SqlParserPos.ZERO),
          SqlKind.OTHER_FUNCTION, ReturnTypes.CURSOR, InferTypes.ANY_NULLABLE,
          Arg.metadata(
              Arg.of("code", f -> f.createSqlType(SqlTypeName.VARCHAR),
                  SqlTypeFamily.STRING, false),
              Arg.of("typeJson", f -> f.createSqlType(SqlTypeName.VARCHAR),
                  SqlTypeFamily.STRING, false)),
          TableFunctionImpl.create(CalciteMorelTableFunction.class, "eval0"));

  @SuppressWarnings("unused") // called via reflection
  public static ScannableTable eval0(String ml, String typeJson) {
    final Ast.Exp e;
    try {
      e = new MorelParserImpl(new StringReader(ml)).expression();
    } catch (ParseException pe) {
      throw new RuntimeException(pe);
    }
    final TypeSystem typeSystem = new TypeSystem();
    final Environment env =
        Environments.env(typeSystem, ImmutableMap.of());
    final Ast.ValDecl valDecl = Compiles.toValDecl(e);
    final TypeResolver.Resolved resolved =
        TypeResolver.deduceType(env, valDecl, typeSystem);
    final Ast.ValDecl valDecl2 = (Ast.ValDecl) resolved.node;
    final Ast.Exp e2 = Compiles.toExp(valDecl2);
    Type type = resolved.typeMap.getType(e2);
    final Code code = new Compiler(resolved.typeMap).compile(env, e2);
    return new ScannableTable() {

      @Override public RelDataType getRowType(RelDataTypeFactory factory) {
        try {
          return RelJsonReader.readType(factory, typeJson);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override public Enumerable<Object[]> scan(DataContext root) {
        final Function<Object, Enumerable<Object[]>> f =
            Converters.toCalciteEnumerable(type, root.getTypeFactory());
        final EvalEnv evalEnv = Codes.emptyEnv();
        Object v = code.eval(evalEnv);
        return f.apply(v);
      }

      @Override public Statistic getStatistic() {
        return Statistics.UNKNOWN;
      }

      @Override public Schema.TableType getJdbcTableType() {
        return Schema.TableType.OTHER;
      }

      @Override public boolean isRolledUp(String column) {
        return false;
      }

      @Override public boolean rolledUpColumnValidInsideAgg(String column,
          SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        return false;
      }
    };
  }

  /** Operand to a user-defined function. */
  private interface Arg {
    String name();
    RelDataType type(RelDataTypeFactory typeFactory);
    SqlTypeFamily family();
    boolean optional();

    static SqlOperandMetadata metadata(Arg... args) {
      return OperandTypes.operandMetadata(
          Arrays.stream(args).map(Arg::family).collect(Collectors.toList()),
          typeFactory ->
              Arrays.stream(args).map(arg -> arg.type(typeFactory))
                  .collect(Collectors.toList()),
          i -> args[i].name(), i -> args[i].optional());
    }

    static Arg of(String name,
        Function<RelDataTypeFactory, RelDataType> protoType,
        SqlTypeFamily family, boolean optional) {
      return new Arg() {
        @Override public String name() {
          return name;
        }

        @Override public RelDataType type(RelDataTypeFactory typeFactory) {
          return protoType.apply(typeFactory);
        }

        @Override public SqlTypeFamily family() {
          return family;
        }

        @Override public boolean optional() {
          return optional;
        }
      };
    }
  }
}

// End CalciteMorelTableFunction.java
