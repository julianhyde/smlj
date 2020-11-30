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
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import net.hydromatic.morel.ast.Ast;
import net.hydromatic.morel.ast.Op;
import net.hydromatic.morel.eval.Code;
import net.hydromatic.morel.eval.EvalEnv;
import net.hydromatic.morel.eval.EvalEnvs;
import net.hydromatic.morel.eval.Unit;
import net.hydromatic.morel.foreign.RelList;
import net.hydromatic.morel.type.Binding;
import net.hydromatic.morel.type.ListType;
import net.hydromatic.morel.type.RecordType;
import net.hydromatic.morel.type.Type;
import net.hydromatic.morel.util.Pair;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

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

  static final Map<Op, SqlOperator> INFIX_OPERATORS =
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
        relBuilder.values(new String[] {"T"}, true);
        yield_(cx, arg);
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
    final Map<String, Function<RelBuilder, RexNode>> map = new HashMap<>();
    if (sourceCodes.size() == 0) {
      relBuilder.values(new String[] {"ZERO"}, 0);
    } else {
      final AtomicInteger i = new AtomicInteger();
      sourceCodes.forEach((pat, r) -> {
        relBuilder.push(r);
        if (pat instanceof Ast.IdPat) {
          relBuilder.as(((Ast.IdPat) pat).name);
          final int inputCount = sourceCodes.size();
          final int input = i.getAndIncrement();
          map.put(((Ast.IdPat) pat).name, b ->
              b.field(inputCount, input, 0));
        }
      });
    }
    Context cx = new Context(env.bindAll(bindings), relBuilder, map);
    for (Ast.FromStep fromStep : from.steps) {
      switch (fromStep.op) {
      case WHERE:
        cx = where(cx, (Ast.Where) fromStep);
        break;
      case ORDER:
        cx = order(cx, (Ast.Order) fromStep);
        break;
      case GROUP:
        cx = group(cx, (Ast.Group) fromStep);
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
      final Binding binding = cx.env.getOpt(id.name);
      if (binding != null && binding.value != Unit.INSTANCE) {
        if (binding.value instanceof Boolean) {
          final Boolean b = (Boolean) binding.value;
          return translate(cx, ast.boolLiteral(id.pos, b));
        }
        if (binding.value instanceof Character) {
          final Character c = (Character) binding.value;
          return translate(cx, ast.charLiteral(id.pos, c));
        }
        if (binding.value instanceof Integer) {
          final BigDecimal bd = BigDecimal.valueOf((Integer) binding.value);
          return translate(cx, ast.intLiteral(id.pos, bd));
        }
        if (binding.value instanceof Float) {
          final BigDecimal bd = BigDecimal.valueOf((Float) binding.value);
          return translate(cx, ast.realLiteral(id.pos, bd));
        }
        if (binding.value instanceof String) {
          final String s = (String) binding.value;
          return translate(cx, ast.stringLiteral(id.pos, s));
        }
      }
      record = toRecord(cx, id);
      if (record != null) {
        return translate(cx, record);
      }
      if (cx.map.containsKey(id.name)) {
        // Not a record, so must be a scalar. It is represented in Calcite
        // as a record with one field.
        final Function<RelBuilder, RexNode> fn = cx.map.get(id.name);
        return fn.apply(cx.relBuilder);
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
      final SqlOperator op = INFIX_OPERATORS.get(infix.op);
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

    throw new AssertionError("cannot translate " + exp.op + " [" + exp + "]");
  }

  private Ast.Record toRecord(Context cx, Ast.Id id) {
    final Type type = cx.env.get(id.name).type;
    if (type instanceof RecordType) {
      final SortedMap<String, Ast.Exp> map = new TreeMap<>();
      ((RecordType) type).argNameTypes.keySet().forEach(field ->
          map.put(field,
              ast.apply(ast.recordSelector(id.pos, field), id)));
      return ast.record(id.pos, map);
    }
    return null;
  }

  private List<RexNode> translateList(Context cx, List<Ast.Exp> exps) {
    return Util.transform(exps, a -> translate(cx, a));
  }

  private Context where(Context cx, Ast.Where where) {
    cx.relBuilder.filter(translate(cx, where.exp));
    return cx;
  }

  private Context order(Context cx, Ast.Order order) {
    final List<RexNode> exps = new ArrayList<>();
    order.orderItems.forEach(i -> {
      RexNode exp = translate(cx, i.exp);
      if (i.direction == Ast.Direction.DESC) {
        exp = cx.relBuilder.desc(exp);
      }
      exps.add(exp);
    });
    cx.relBuilder.sort(exps);
    return cx;
  }

  private Context group(Context cx, Ast.Group group) {
    final Map<String, Function<RelBuilder, RexNode>> map = new HashMap<>();
    final List<Binding> bindings = new ArrayList<>();
    final List<RexNode> nodes = new ArrayList<>();
    final AtomicInteger ai = new AtomicInteger();
    Pair.forEach(group.groupExps, (id, exp) -> {
      bindings.add(Binding.of(id.name, typeMap.getType(id)));
      nodes.add(translate(cx, exp));
      final int i = ai.getAndIncrement();
      map.put(id.name, b -> b.field(1, 0, i));
    });
    final RelBuilder.GroupKey groupKey = cx.relBuilder.groupKey(nodes);
    final List<RelBuilder.AggCall> aggregateCalls = new ArrayList<>();
    group.aggregates.forEach(aggregate -> {
      bindings.add(Binding.of(aggregate.id.name, typeMap.getType(aggregate)));
      aggregateCalls.add(
          cx.relBuilder.aggregateCall(SqlStdOperatorTable.SUM, // TODO:
              aggregate.argument == null
              ? ImmutableList.of()
              : ImmutableList.of(translate(cx, aggregate.argument)))
              .as(aggregate.id.name));
      final int i = ai.getAndIncrement();
      map.put(aggregate.id.name, b -> b.field(1, 0, i));
    });
    cx.relBuilder.aggregate(groupKey, aggregateCalls);
    return new Context(cx.env.bindAll(bindings), cx.relBuilder, map);
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
    final Map<String, Function<RelBuilder, RexNode>> map;

    Context(Environment env, RelBuilder relBuilder,
        Map<String, Function<RelBuilder, RexNode>> map) {
      this.env = env;
      this.relBuilder = relBuilder;
      this.map = map;
    }
  }
}

// End CalciteCompiler.java
