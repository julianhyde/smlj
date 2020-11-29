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
package net.hydromatic.morel.compile;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;

import net.hydromatic.morel.ast.Ast;
import net.hydromatic.morel.ast.Op;
import net.hydromatic.morel.eval.Code;
import net.hydromatic.morel.eval.EvalEnv;
import net.hydromatic.morel.eval.EvalEnvs;
import net.hydromatic.morel.foreign.RelList;
import net.hydromatic.morel.type.Binding;
import net.hydromatic.morel.type.ListType;
import net.hydromatic.morel.type.RecordType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static net.hydromatic.morel.ast.AstBuilder.ast;

/** Compiles an expression to code that can be evaluated. */
public class CalciteCompiler extends Compiler {
  /** Morel operators and their exact equivalents in Calcite. */
  static final Map<String, SqlOperator> DIRECT_OPS =
      ImmutableMap.<String, SqlOperator>builder()
          .put("op =", SqlStdOperatorTable.EQUALS)
          .put("op <>", SqlStdOperatorTable.NOT_EQUALS)
          .put("op <", SqlStdOperatorTable.LESS_THAN)
          .put("op <=", SqlStdOperatorTable.LESS_THAN_OR_EQUAL)
          .put("op >", SqlStdOperatorTable.GREATER_THAN)
          .put("op >=", SqlStdOperatorTable.GREATER_THAN_OR_EQUAL)
          .put("op +", SqlStdOperatorTable.PLUS)
          .put("op -", SqlStdOperatorTable.MINUS)
          .put("op ~", SqlStdOperatorTable.UNARY_MINUS)
          .put("op *", SqlStdOperatorTable.MULTIPLY)
          .put("op /", SqlStdOperatorTable.DIVIDE)
          .put("op mod", SqlStdOperatorTable.MOD)
          .build();

  static final Map<Op, SqlOperator> DIRECT_OPS2 =
      ImmutableMap.<Op, SqlOperator>builder()
          .put(Op.ANDALSO, SqlStdOperatorTable.AND)
          .put(Op.ORELSE, SqlStdOperatorTable.OR)
          .build();

  public CalciteCompiler(TypeMap typeMap) {
    super(typeMap);
  }

  public RelNode toRel(Environment env, Ast.Exp expression) {
    final FrameworkConfig config = Frameworks.newConfigBuilder().build();
    final RelBuilder relBuilder = RelBuilder.create(config);
    switch (expression.op) {
    case FROM:
      return from(env, relBuilder, (Ast.From) expression).build();

    case LIST:
      // For example, 'from n in [1, 2, 3]'
      final Ast.List list = (Ast.List) expression;
      final Context cx = new Context(env, relBuilder, ImmutableMap.of());
      for (Ast.Exp arg : list.args) {
        relBuilder.values(new String[] {"T"}, true)
            .project(translate(cx, arg));
      }
      return relBuilder.union(true, list.args.size()).build();

    case APPLY:
      final Ast.Apply apply = (Ast.Apply) expression;
      if (apply.fn instanceof Ast.RecordSelector
          && apply.arg instanceof Ast.Id) {
        // Something like '#emp scott', 'scott' is a foreign value
        final Code code1 = compile(env, apply);
        final Object o = code1.eval(evalEnvOf(env));
        if (o instanceof RelList) {
          return ((RelList) o).rel;
        }
      }
      // fall through

    default:
      throw new AssertionError("unknown: " + expression);
    }
  }

  private RelBuilder from(Environment env, RelBuilder relBuilder, Ast.From from) {
    final Map<Ast.Pat, RelNode> sourceCodes = new LinkedHashMap<>();
    final List<Binding> bindings = new ArrayList<>();
    for (Map.Entry<Ast.Pat, Ast.Exp> patExp : from.sources.entrySet()) {
      final RelNode expCode = toRel(env.bindAll(bindings), patExp.getValue());
      final Ast.Pat pat0 = patExp.getKey();
      final ListType listType = (ListType) typeMap.getType(patExp.getValue());
      final Ast.Pat pat = expandRecordPattern(pat0, listType.elementType);
      sourceCodes.put(pat, expCode);
      pat.visit(p -> {
        if (p instanceof Ast.IdPat) {
          final Ast.IdPat idPat = (Ast.IdPat) p;
          bindings.add(Binding.of(idPat.name, typeMap.getType(p)));
        }
      });
    }
    final Map<String, Pair<Integer, RelDataType>> map = new HashMap<>();
    if (sourceCodes.size() == 0) {
      relBuilder.values(new String[] {"ZERO"}, 0);
    } else {
      final AtomicInteger i = new AtomicInteger();
      sourceCodes.forEach((pat, r) -> {
        relBuilder.push(r);
        if (pat instanceof Ast.IdPat) {
          relBuilder.as(((Ast.IdPat) pat).name);
          map.put(((Ast.IdPat) pat).name,
              Pair.of(i.getAndAdd(r.getRowType().getFieldCount()),
                  r.getRowType()));
        }
      });
    }
    final Context cx = new Context(env.bindAll(bindings), relBuilder, map);
    for (Ast.FromStep fromStep : from.steps) {
      switch (fromStep.op) {
      case WHERE:
        where(cx, (Ast.Where) fromStep);
        break;
      default:
        throw new AssertionError(fromStep);
      }
    }
    if (from.yieldExp != null) {
      yield_(cx, from.yieldExp);
    }
    return relBuilder;
  }

  private RelBuilder yield_(Context cx, Ast.Exp exp) {
    final Ast.Record record;
    switch (exp.op) {
    case ID:
      final Ast.Id id = (Ast.Id) exp;
      record = toRecord(cx, id);
      if (record != null) {
        return yield_(cx, record);
      }
      break;

    case RECORD:
      record = (Ast.Record) exp;
      return cx.relBuilder.project(
          Util.transform(record.args.values(), e -> translate(cx, e)),
          record.args.keySet());
    }
    RexNode rex = translate(cx, exp);
    return cx.relBuilder.project(rex);
  }

  private RexNode translate(Context cx, Ast.Exp exp) {
    final Ast.Record record;
    switch (exp.op) {
    case BOOL_LITERAL:
    case CHAR_LITERAL:
    case INT_LITERAL:
    case REAL_LITERAL:
    case STRING_LITERAL:
    case UNIT_LITERAL:
      final Ast.Literal literal = (Ast.Literal) exp;
      switch (exp.op) {
      case CHAR_LITERAL:
        // convert from Character to singleton String
        return cx.relBuilder.literal(literal.value + "");
      case UNIT_LITERAL:
        return cx.relBuilder.call(SqlStdOperatorTable.ROW);
      default:
        return cx.relBuilder.literal(literal.value);
      }

    case ID:
      // In 'from e in emps yield e', 'e' expands to a record,
      // '{e.deptno, e.ename}'
      final Ast.Id id = (Ast.Id) exp;
      record = toRecord(cx, id);
      if (record != null) {
        return translate(cx, record);
      }
      if (cx.map.containsKey(id.name)) {
        // Not a record, so must be a scalar. It is represented in Calcite
        // as a record with one field.
        final Pair<Integer, RelDataType> pair = cx.map.get(id.name);
        assert pair.right.getFieldCount() == 1;
        return cx.relBuilder.field(cx.map.size(), pair.left, 0);
      }
      if (id.name.equals("true") || id.name.equals("false")) {
        return translate(cx, ast.boolLiteral(id.pos, id.name.equals("true")));
      }
      break;

    case APPLY:
      final Ast.Apply apply = (Ast.Apply) exp;
      if (apply.fn instanceof Ast.RecordSelector
          && apply.arg instanceof Ast.Id) {
        // Something like '#deptno e',
        return cx.relBuilder.field(((Ast.Id) apply.arg).name,
            ((Ast.RecordSelector) apply.fn).name);
      }
      if (apply.fn instanceof Ast.Id) {
        final Ast.Tuple tuple = (Ast.Tuple) apply.arg;
        final SqlOperator op = DIRECT_OPS.get(((Ast.Id) apply.fn).name);
        if (op != null) {
          return cx.relBuilder.call(op, translateList(cx, tuple.args()));
        }
      }
      break;

    case ANDALSO:
    case ORELSE:
      final Ast.InfixCall infix = (Ast.InfixCall) exp;
      final SqlOperator op = DIRECT_OPS2.get(infix.op);
      return cx.relBuilder.call(op, translateList(cx, infix.args()));

    case RECORD:
      record = (Ast.Record) exp;
      final RelDataTypeFactory.Builder builder =
          cx.relBuilder.getTypeFactory().builder();
      final List<RexNode> operands = new ArrayList<>();
      record.args.forEach((name, arg) -> {
        final RexNode e = translate(cx, arg);
        operands.add(e);
        builder.add(name, e.getType());
      });
      final RelDataType type = builder.build();
      return cx.relBuilder.getRexBuilder().makeCall(type,
          SqlStdOperatorTable.ROW, operands);
    }

    throw new AssertionError(exp);
  }

  private Ast.Record toRecord(Context cx, Ast.Id id) {
    if (cx.map.containsKey(id.name)
        && cx.env.get(id.name).type instanceof RecordType) {
      final Pair<Integer, RelDataType> pair = cx.map.get(id.name);
      final SortedMap<String, Ast.Exp> map = new TreeMap<>();
      pair.right.getFieldList().forEach(field ->
          map.put(field.getName(),
              ast.apply(ast.recordSelector(id.pos, field.getName()), id)));
      return ast.record(id.pos, map);
    }
    return null;
  }

  private Iterable<RexNode> translateList(Context cx, Collection<Ast.Exp> exps) {
    return Util.transform(exps, a -> translate(cx, a));
  }

  private void where(Context cx, Ast.Where where) {
    cx.relBuilder.filter(translate(cx, where.exp));
  }

  private static EvalEnv evalEnvOf(Environment env) {
    final Map<String, Object> map = new HashMap<>();
    env.forEachValue(map::put);
    EMPTY_ENV.visit(map::putIfAbsent);
    return EvalEnvs.copyOf(map);
  }

  /** Translation context. */
  static class Context {
    final Environment env;
    final RelBuilder relBuilder;
    final Map<String, Pair<Integer, RelDataType>> map;

    Context(Environment env, RelBuilder relBuilder,
        Map<String, Pair<Integer, RelDataType>> map) {
      this.env = env;
      this.relBuilder = relBuilder;
      this.map = map;
    }
  }
}

// End CalciteCompiler.java
