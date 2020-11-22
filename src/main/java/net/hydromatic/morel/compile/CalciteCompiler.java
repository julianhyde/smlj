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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import net.hydromatic.morel.ast.Ast;
import net.hydromatic.morel.eval.Code;
import net.hydromatic.morel.eval.EvalEnv;
import net.hydromatic.morel.eval.EvalEnvs;
import net.hydromatic.morel.foreign.RelList;
import net.hydromatic.morel.type.Binding;
import net.hydromatic.morel.type.ListType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    if (sourceCodes.size() == 0) {
      relBuilder.values(new String[] {"ZERO"}, 0);
    } else {
      sourceCodes.forEach((pat, r) -> {
        relBuilder.push(r);
        if (pat instanceof Ast.IdPat) {
          relBuilder.as(((Ast.IdPat) pat).name);
        }
      });
    }
    for (Ast.FromStep fromStep : from.steps) {
      switch (fromStep.op) {
      case WHERE:
        where(env, relBuilder, (Ast.Where) fromStep);
        break;
      }
    }
    if (from.yieldExp != null) {
      project(env, relBuilder, from.yieldExp);
    }
    return relBuilder;
  }

  private RelBuilder project(Environment env, RelBuilder relBuilder,
      Ast.Exp exp) {
    RexNode rex = translate(env, relBuilder, exp);
    return relBuilder.project(rex);
  }

  private RexNode translate(Environment env, RelBuilder relBuilder,
      Ast.Exp exp) {
    switch (exp.op) {
    case APPLY:
      final Ast.Apply apply = (Ast.Apply) exp;
      if (apply.fn instanceof Ast.RecordSelector
          && apply.arg instanceof Ast.Id) {
        // Something like '#deptno e',
        return relBuilder.field(((Ast.Id) apply.arg).name,
            ((Ast.RecordSelector) apply.fn).name.toUpperCase(Locale.ROOT));
      }
      // fall through
    default:
      throw new AssertionError();
    }
  }

  private void where(Environment env, RelBuilder relBuilder, Ast.Where where) {

  }

  private static EvalEnv evalEnvOf(Environment env) {
    final Map<String, Object> map = new HashMap<>();
    env.forEachValue(map::put);
    EMPTY_ENV.visit(map::putIfAbsent);
    return EvalEnvs.copyOf(map);
  }

  /** Utilities for creating various kinds of {@link RelNode}. */
  private static class RelNodes {

  }
}

// End CalciteCompiler.java
