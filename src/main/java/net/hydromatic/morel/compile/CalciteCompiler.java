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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;

import net.hydromatic.morel.ast.Ast;
import net.hydromatic.morel.eval.Code;
import net.hydromatic.morel.eval.EvalEnv;
import net.hydromatic.morel.eval.EvalEnvs;
import net.hydromatic.morel.foreign.RelList;
import net.hydromatic.morel.type.Binding;
import net.hydromatic.morel.type.ListType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static net.hydromatic.morel.ast.AstBuilder.ast;

/** Compiles an expression to code that can be evaluated. */
public class CalciteCompiler extends Compiler {
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
    final Context cx = new Context(env, relBuilder, map);
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
      project(cx, from.yieldExp);
    }
    return relBuilder;
  }

  private RelBuilder project(Context cx, Ast.Exp exp) {
    RexNode rex = translate(cx, exp);
    if (rex.getKind() == SqlKind.ROW) {
      return cx.relBuilder.project(((RexCall) rex).operands);
    } else {
      return cx.relBuilder.project(rex);
    }
  }

  private RexNode translate(Context cx, Ast.Exp exp) {
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
      if (cx.map.containsKey(id.name)) {
        final Pair<Integer, RelDataType> pair = cx.map.get(id.name);
        final List<RexNode> exps = new ArrayList<>();
        for (RelDataTypeField field : pair.right.getFieldList()) {
          exps.add(
              cx.relBuilder.alias(
                  cx.relBuilder.field(cx.map.size(), pair.left,
                      field.getIndex()),
                  field.getName()));
        }
        return cx.relBuilder.call(SqlStdOperatorTable.ROW, exps);
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
        if (((Ast.Id) apply.fn).name.equals("op +")) {
          final Ast.Tuple tuple = (Ast.Tuple) apply.arg;
          return cx.relBuilder.call(SqlStdOperatorTable.PLUS,
              getTransform(cx, tuple.args));
        }
      }
      break;

    case RECORD:
      final Ast.Record record = (Ast.Record) exp;
      return cx.relBuilder.call(SqlStdOperatorTable.ROW,
          getTransform(cx, record.args));
    }

    throw new AssertionError(exp);
  }

  private Iterable<RexNode> getTransform(Context cx, Collection<Ast.Exp> exps) {
    return Util.transform(exps, a -> translate(cx, a));
  }

  private Iterable<RexNode> getTransform(Context cx,
      Map<String, Ast.Exp> exps) {
    return Util.transform(exps.entrySet(), entry ->
        cx.relBuilder.alias(translate(cx, entry.getValue()), entry.getKey()));
  }

  private void where(Context cx, Ast.Where where) {
    // TODO:
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
