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

import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schemas;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import net.hydromatic.morel.ast.Ast;
import net.hydromatic.morel.ast.Op;
import net.hydromatic.morel.ast.Pos;
import net.hydromatic.morel.eval.Applicable;
import net.hydromatic.morel.eval.Closure;
import net.hydromatic.morel.eval.Code;
import net.hydromatic.morel.eval.Codes;
import net.hydromatic.morel.eval.Describer;
import net.hydromatic.morel.eval.EvalEnv;
import net.hydromatic.morel.eval.Session;
import net.hydromatic.morel.eval.Unit;
import net.hydromatic.morel.foreign.Converters;
import net.hydromatic.morel.type.Binding;
import net.hydromatic.morel.type.DataType;
import net.hydromatic.morel.type.ListType;
import net.hydromatic.morel.type.RecordType;
import net.hydromatic.morel.type.Type;
import net.hydromatic.morel.util.Ord;
import net.hydromatic.morel.util.Pair;
import net.hydromatic.morel.util.TailList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;

import static net.hydromatic.morel.ast.Ast.Direction.DESC;
import static net.hydromatic.morel.ast.AstBuilder.ast;
import static net.hydromatic.morel.util.Static.toImmutableList;

/** Compiles an expression to code that can be evaluated. */
public class Compiler {
  protected static final EvalEnv EMPTY_ENV = Codes.emptyEnv();

  protected final TypeMap typeMap;

  public Compiler(TypeMap typeMap) {
    this.typeMap = typeMap;
  }

  CompiledStatement compileStatement(Environment env, Ast.Decl decl) {
    final List<Code> varCodes = new ArrayList<>();
    final List<Binding> bindings = new ArrayList<>();
    final List<Action> actions = new ArrayList<>();
    final Context cx = Context.of(env);
    compileDecl(cx, decl, varCodes, bindings, actions);
    final Type type = typeMap.getType(decl);

    return new CompiledStatement() {
      public Type getType() {
        return type;
      }

      public void eval(Session session, Environment env, List<String> output,
          List<Binding> bindings) {
        final EvalEnv evalEnv = Codes.emptyEnvWith(session, env);
        for (Action entry : actions) {
          entry.apply(output, bindings, evalEnv);
        }
      }
    };
  }

  /** Something that needs to happen when a declaration is evaluated.
   *
   * <p>Usually involves placing a type or value into the bindings that will
   * make up the environment in which the next statement will be executed, and
   * printing some text on the screen. */
  interface Action {
    void apply(List<String> output, List<Binding> bindings, EvalEnv evalEnv);
  }

  /** Compilation context. */
  static class Context {
    final Environment env;

    Context(Environment env) {
      this.env = env;
    }

    static Context of(Environment env) {
      return new Context(env);
    }

    Context bindAll(Iterable<Binding> bindings) {
      return of(env.bindAll(bindings));
    }

    Context bind(String name, Type type, Object value) {
      return of(env.bind(name, type, value));
    }
  }

  public final Code compile(Environment env, Ast.Exp expression) {
    return compile(Context.of(env), expression);
  }

  public Code compile(Context cx, Ast.Exp expression) {
    final Ast.Literal literal;
    final Code argCode;
    final List<Code> codes;
    switch (expression.op) {
    case BOOL_LITERAL:
      literal = (Ast.Literal) expression;
      final Boolean boolValue = (Boolean) literal.value;
      return Codes.constant(boolValue);

    case CHAR_LITERAL:
      literal = (Ast.Literal) expression;
      final Character charValue = (Character) literal.value;
      return Codes.constant(charValue);

    case INT_LITERAL:
      literal = (Ast.Literal) expression;
      return Codes.constant(((BigDecimal) literal.value).intValue());

    case REAL_LITERAL:
      literal = (Ast.Literal) expression;
      return Codes.constant(((BigDecimal) literal.value).floatValue());

    case STRING_LITERAL:
      literal = (Ast.Literal) expression;
      final String stringValue = (String) literal.value;
      return Codes.constant(stringValue);

    case UNIT_LITERAL:
      return Codes.constant(Unit.INSTANCE);

    case IF:
      final Ast.If if_ = (Ast.If) expression;
      final Code conditionCode = compile(cx, if_.condition);
      final Code trueCode = compile(cx, if_.ifTrue);
      final Code falseCode = compile(cx, if_.ifFalse);
      return Codes.ifThenElse(conditionCode, trueCode, falseCode);

    case LET:
      return compileLet(cx, (Ast.LetExp) expression);

    case FN:
      final Ast.Fn fn = (Ast.Fn) expression;
      return compileMatchList(cx, fn.matchList);

    case CASE:
      final Ast.Case case_ = (Ast.Case) expression;
      final Code matchCode = compileMatchList(cx, case_.matchList);
      argCode = compile(cx, case_.e);
      return Codes.apply(matchCode, argCode);

    case RECORD_SELECTOR:
      final Ast.RecordSelector recordSelector = (Ast.RecordSelector) expression;
      return Codes.nth(recordSelector.slot).asCode();

    case APPLY:
      return compileApply(cx, (Ast.Apply) expression);

    case LIST:
      return compileList(cx, (Ast.List) expression);

    case FROM:
      return compileFrom(cx, (Ast.From) expression);

    case ID:
      final Ast.Id id = (Ast.Id) expression;
      final Binding binding = cx.env.getOpt(id.name);
      if (binding != null && binding.value instanceof Code) {
        return (Code) binding.value;
      }
      return Codes.get(id.name);

    case TUPLE:
      final Ast.Tuple tuple = (Ast.Tuple) expression;
      codes = new ArrayList<>();
      for (Ast.Exp arg : tuple.args) {
        codes.add(compile(cx, arg));
      }
      return Codes.tuple(codes);

    case RECORD:
      final Ast.Record record = (Ast.Record) expression;
      return compile(cx, ast.tuple(record.pos, record.args.values()));

    case ANDALSO:
    case ORELSE:
    case CONS:
      return compileInfix(cx, (Ast.InfixCall) expression,
          typeMap.getType(expression));

    default:
      throw new AssertionError("op not handled: " + expression.op);
    }
  }

  protected Code compileList(Context cx, Ast.List list) {
    final List<Code> codes = new ArrayList<>();
    for (Ast.Exp arg : list.args) {
      codes.add(compile(cx, arg));
    }
    return Codes.list(codes);
  }

  protected Code compileApply(Context cx, Ast.Apply apply) {
    Code argCode;
    assignSelector(apply);
    argCode = compile(cx, apply.arg);
    final Type argType = typeMap.getType(apply.arg);
    final Applicable fnValue = compileApplicable(cx, apply.fn, argType);
    if (fnValue != null) {
      return Codes.apply(fnValue, argCode);
    }
    final Code fnCode = compile(cx, apply.fn);
    return Codes.apply(fnCode, argCode);
  }

  protected Code compileFrom(Context cx, Ast.From from) {
    final Map<Ast.Pat, Code> sourceCodes = new LinkedHashMap<>();
    final List<Binding> bindings = new ArrayList<>();
    for (Map.Entry<Ast.Pat, Ast.Exp> patExp : from.sources.entrySet()) {
      final Code expCode = compile(cx.bindAll(bindings), patExp.getValue());
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
    Supplier<Codes.RowSink> rowSinkFactory =
        createRowSinkFactory(cx, ImmutableList.copyOf(bindings), from.steps,
            from.yieldExpOrDefault);
    return Codes.from(sourceCodes, rowSinkFactory);
  }

  protected Supplier<Codes.RowSink> createRowSinkFactory(Context cx0,
      ImmutableList<Binding> bindings, List<Ast.FromStep> steps,
      Ast.Exp yieldExp) {
    final Context cx = cx0.bindAll(bindings);
    if (steps.isEmpty()) {
      final Code yieldCode = compile(cx, yieldExp);
      return () -> Codes.yieldRowSink(yieldCode);
    }
    final Ast.FromStep firstStep = steps.get(0);
    final ImmutableList.Builder<Binding> outBindingBuilder =
        ImmutableList.builder();
    firstStep.deriveOutBindings(bindings,
        (name, ast) -> Binding.of(name, typeMap.getType(ast)),
        outBindingBuilder::add);
    final ImmutableList<Binding> outBindings = outBindingBuilder.build();
    final Supplier<Codes.RowSink> nextFactory =
        createRowSinkFactory(cx, outBindings,
            steps.subList(1, steps.size()), yieldExp);
    switch (firstStep.op) {
    case WHERE:
      final Ast.Where where = (Ast.Where) firstStep;
      final Code filterCode = compile(cx, where.exp);
      return () -> Codes.whereRowSink(filterCode, nextFactory.get());

    case ORDER:
      final Ast.Order order = (Ast.Order) firstStep;
      final ImmutableList<Pair<Code, Boolean>> codes =
          order.orderItems.stream()
              .map(i -> Pair.of(compile(cx, i.exp), i.direction == DESC))
              .collect(toImmutableList());
      return () -> Codes.orderRowSink(codes, bindings, nextFactory.get());

    case GROUP:
      final Ast.Group group = (Ast.Group) firstStep;
      final ImmutableList.Builder<Code> groupCodesB = ImmutableList.builder();
      for (Pair<Ast.Id, Ast.Exp> pair : group.groupExps) {
        groupCodesB.add(compile(cx, pair.right));
      }
      final ImmutableList<String> names = bindingNames(bindings);
      final ImmutableList.Builder<Applicable> aggregateCodesB =
          ImmutableList.builder();
      for (Ast.Aggregate aggregate : group.aggregates) {
        final Code argumentCode;
        final Type argumentType;
        if (aggregate.argument == null) {
          final SortedMap<String, Type> argNameTypes =
              new TreeMap<>(RecordType.ORDERING);
          bindings.forEach(b -> argNameTypes.put(b.name, b.type));
          argumentType = typeMap.typeSystem.recordOrScalarType(argNameTypes);
          argumentCode = null;
        } else {
          argumentType = typeMap.getType(aggregate.argument);
          argumentCode = compile(cx, aggregate.argument);
        }
        final Applicable aggregateApplicable =
            compileApplicable(cx, aggregate.aggregate,
                typeMap.typeSystem.listType(argumentType));
        final Code aggregateCode;
        if (aggregateApplicable == null) {
          aggregateCode = compile(cx, aggregate.aggregate);
        } else {
          aggregateCode = aggregateApplicable.asCode();
        }
        aggregateCodesB.add(
            Codes.aggregate(cx.env, aggregateCode, names, argumentCode));
      }
      final ImmutableList<Code> groupCodes = groupCodesB.build();
      final Code keyCode = Codes.tuple(groupCodes);
      final ImmutableList<Applicable> aggregateCodes = aggregateCodesB.build();
      final ImmutableList<String> outNames = bindingNames(outBindings);
      return () -> Codes.groupRowSink(keyCode, aggregateCodes, names,
          outNames, nextFactory.get());

    default:
      throw new AssertionError("unknown step type " + firstStep.op);
    }
  }

  private ImmutableList<String> bindingNames(List<Binding> bindings) {
    //noinspection UnstableApiUsage
    return bindings.stream().map(b -> b.name)
        .collect(ImmutableList.toImmutableList());
  }

  private void assignSelector(Ast.Apply apply) {
    if (apply.fn instanceof Ast.RecordSelector) {
      final Ast.RecordSelector selector = (Ast.RecordSelector) apply.fn;
      if (selector.slot < 0) {
        final Type argType = typeMap.getType(apply.arg);
        if (argType instanceof RecordType) {
          final RecordType recordType = (RecordType) argType;
          final Ord<Type> field = recordType.lookupField(selector.name);
          selector.slot = field.i;
        }
      }
    }
  }

  /** Compiles a function value to an {@link Applicable}, if possible, or
   * returns null. */
  private Applicable compileApplicable(Context cx, Ast.Exp fn,
      Type argType) {
    final Binding binding = getConstant(cx, fn);
    if (binding != null) {
      if (binding.value instanceof Macro) {
        final Ast.Exp e = ((Macro) binding.value).expand(cx.env, argType);
        switch (e.op) {
        case WRAPPED_APPLICABLE:
          return ((Ast.ApplicableExp) e).applicable;
        }
        final Code code = compile(cx, e);
        return new Applicable() {
          @Override public Describer describe(Describer describer) {
            return code.describe(describer);
          }

          @Override public Object apply(EvalEnv evalEnv, Object arg) {
            return code.eval(evalEnv);
          }
        };
      }
      if (binding.value instanceof Applicable) {
        return (Applicable) binding.value;
      }
    }
    final Code fnCode = compile(cx, fn);
    if (fnCode.isConstant()) {
      return (Applicable) fnCode.eval(EMPTY_ENV);
    } else {
      return null;
    }
  }

  private Binding getConstant(Context cx, Ast.Exp fn) {
    switch (fn.op) {
    case ID:
      return cx.env.getOpt(((Ast.Id) fn).name);
    case APPLY:
      final Ast.Apply apply = (Ast.Apply) fn;
      if (apply.fn.op == Op.RECORD_SELECTOR) {
        final Ast.RecordSelector recordSelector = (Ast.RecordSelector) apply.fn;
        final Binding argBinding = getConstant(cx, apply.arg);
        if (argBinding != null
            && argBinding.value != Unit.INSTANCE
            && argBinding.type instanceof RecordType) {
          final RecordType recordType = (RecordType) argBinding.type;
          final Ord<Type> field = recordType.lookupField(recordSelector.name);
          if (field != null) {
            @SuppressWarnings("rawtypes")
            final List list = (List) argBinding.value;
            return Binding.of(recordSelector.name, field.e, list.get(field.i));
          }
        }
      }
      // fall through
    default:
      return null;
    }
  }

  private Code compileAggregate(Context cx, Ast.Aggregate aggregate) {
    throw new UnsupportedOperationException(); // TODO
  }

  private Code compileLet(Context cx, Ast.LetExp let) {
    return compileLet(cx, let.decls, let.e);
  }

  private Code compileLet(Context cx, List<Ast.Decl> decls, Ast.Exp e) {
    final Ast.LetExp letExp = flattenLet(decls, e);
    return compileLet(cx, Iterables.getOnlyElement(letExp.decls), letExp.e);
  }

  protected Code compileLet(Context cx, Ast.Decl decl, Ast.Exp e) {
    final List<Code> varCodes = new ArrayList<>();
    final List<Binding> bindings = new ArrayList<>();
    compileDecl(cx, decl, varCodes, bindings, null);
    Context cx2 = cx.bindAll(bindings);
    final Code resultCode = compile(cx2, e);
    return Codes.let(varCodes, resultCode);
  }

  protected Ast.LetExp flattenLet(List<Ast.Decl> decls, Ast.Exp e) {
    if (decls.size() == 1) {
      return ast.let(e.pos, decls, e);
    } else {
      return ast.let(e.pos, decls.subList(0, 1),
          flattenLet(decls.subList(1, decls.size()), e));
    }
  }

  void compileDecl(Context cx, Ast.Decl decl, List<Code> varCodes,
      List<Binding> bindings, List<Action> actions) {
    switch (decl.op) {
    case VAL_DECL:
      compileValDecl(cx, (Ast.ValDecl) decl, varCodes, bindings, actions);
      break;
    case DATATYPE_DECL:
      final Ast.DatatypeDecl datatypeDecl = (Ast.DatatypeDecl) decl;
      compileDatatypeDecl(cx, datatypeDecl, bindings, actions);
      break;
    case FUN_DECL:
      throw new AssertionError("unknown " + decl.op + " [" + decl
          + "] (did you remember to call TypeResolver.toValDecl?)");
    default:
      throw new AssertionError("unknown " + decl.op + " [" + decl + "]");
    }
  }

  private void compileValDecl(Context cx, Ast.ValDecl valDecl,
      List<Code> varCodes, List<Binding> bindings, List<Action> actions) {
    if (valDecl.valBinds.size() > 1) {
      // Transform "let val v1 = e1 and v2 = e2 in e"
      // to "let val (v1, v2) = (e1, e2) in e"
      final Map<Ast.Pat, Ast.Exp> matches = new LinkedHashMap<>();
      boolean rec = false;
      for (Ast.ValBind valBind : valDecl.valBinds) {
        flatten(matches, valBind.pat, valBind.e);
        rec |= valBind.rec;
      }
      final Pos pos = valDecl.pos;
      final Ast.Pat pat = ast.tuplePat(pos, matches.keySet());
      final Ast.Exp e2 = ast.tuple(pos, matches.values());
      valDecl = ast.valDecl(pos, ast.valBind(pos, rec, pat, e2));
      matches.forEach((key, value) ->
          bindings.add(
              Binding.of(((Ast.IdPat) key).name, typeMap.getType(value),
                  Unit.INSTANCE)));
    } else {
      valDecl.valBinds.forEach(valBind ->
          valBind.pat.visit(pat -> {
            if (pat instanceof Ast.IdPat) {
              final Type paramType = typeMap.getType(pat);
              bindings.add(Binding.of(((Ast.IdPat) pat).name, paramType));
            }
          }));
    }
    for (Ast.ValBind valBind : valDecl.valBinds) {
      compileValBind(cx, valBind, varCodes, bindings, actions);
    }
  }

  private void compileDatatypeDecl(Context cx,
      Ast.DatatypeDecl datatypeDecl, List<Binding> bindings,
      List<Action> actions) {
    for (Ast.DatatypeBind bind : datatypeDecl.binds) {
      final List<Binding> newBindings = new TailList<>(bindings);
      final DataType dataType =
          (DataType) typeMap.typeSystem.lookup(bind.name.name);
      for (Ast.TyCon tyCon : bind.tyCons) {
        bindings.add(typeMap.typeSystem.bindTyCon(dataType, tyCon.id.name));
      }
      if (actions != null) {
        final List<Binding> immutableBindings =
            ImmutableList.copyOf(newBindings);
        actions.add((output, outBindings, evalEnv) -> {
          output.add("datatype " + bind);
          outBindings.addAll(immutableBindings);
        });
      }
    }
  }

  private Code compileInfix(Context cx, Ast.InfixCall call, Type type) {
    final Code code0 = compile(cx, call.a0);
    final Code code1 = compile(cx, call.a1);
    switch (call.op) {
    case ANDALSO:
      return Codes.andAlso(code0, code1);
    case ORELSE:
      return Codes.orElse(code0, code1);
    default:
      throw new AssertionError("unknown op " + call.op);
    }
  }

  private void flatten(Map<Ast.Pat, Ast.Exp> matches,
      Ast.Pat pat, Ast.Exp exp) {
    switch (pat.op) {
    case TUPLE_PAT:
      final Ast.TuplePat tuplePat = (Ast.TuplePat) pat;
      if (exp.op == Op.TUPLE) {
        final Ast.Tuple tuple = (Ast.Tuple) exp;
        Pair.forEach(tuplePat.args, tuple.args,
            (p, e) -> flatten(matches, p, e));
        break;
      }
      // fall through
    default:
      matches.put(pat, exp);
    }
  }

  /** Compiles a {@code match} expression.
   *
   * @param cx Compile context
   * @param matchList List of Match
   * @return Code for match
   */
  private Code compileMatchList(Context cx,
      List<Ast.Match> matchList) {
    @SuppressWarnings("UnstableApiUsage")
    final ImmutableList<Pair<Ast.Pat, Code>> patCodes =
        matchList.stream()
            .map(match -> compileMatch(cx, match))
            .collect(ImmutableList.toImmutableList());
    return new MatchCode(patCodes);
  }

  private Pair<Ast.Pat, Code> compileMatch(Context cx, Ast.Match match) {
    final Context[] envHolder = {cx};
    match.pat.visit(pat -> {
      if (pat instanceof Ast.IdPat) {
        final Type paramType = typeMap.getType(pat);
        envHolder[0] = envHolder[0].bind(((Ast.IdPat) pat).name,
            paramType, Unit.INSTANCE);
      }
    });
    final Code code = compile(envHolder[0], match.e);
    final Type type = typeMap.getTypeOpt(match.pat);
    return Pair.of(expandRecordPattern(match.pat, type), code);
  }

  /** Expands a pattern if it is a record pattern that has an ellipsis
   * or if the arguments are not in the same order as the labels in the type. */
  protected final Ast.Pat expandRecordPattern(Ast.Pat pat, Type type) {
    switch (pat.op) {
    case ID_PAT:
      final Ast.IdPat idPat = (Ast.IdPat) pat;
      if (type.op() == Op.DATA_TYPE
          && ((DataType) type).typeConstructors.containsKey(idPat.name)) {
        return ast.con0Pat(idPat.pos, ast.id(idPat.pos, idPat.name));
      }
      return pat;

    case RECORD_PAT:
      final RecordType recordType = (RecordType) type;
      final Ast.RecordPat recordPat = (Ast.RecordPat) pat;
      final Map<String, Ast.Pat> args = new LinkedHashMap<>();
      for (String label : recordType.argNameTypes.keySet()) {
        args.put(label,
            recordPat.args.getOrDefault(label, ast.wildcardPat(pat.pos)));
      }
      if (recordPat.ellipsis || !recordPat.args.equals(args)) {
        // Only create an expanded pattern if it is different (no ellipsis,
        // or arguments in a different order).
        return ast.recordPat(recordPat.pos, false, args);
      }
      // fall through
      return recordPat;
    default:
      return pat;
    }
  }

  private void compileValBind(Context cx, Ast.ValBind valBind,
      List<Code> varCodes, List<Binding> bindings, List<Action> actions) {
    final List<Binding> newBindings = new TailList<>(bindings);
    final Code code0;
    if (valBind.rec) {
      final Map<Ast.IdPat, LinkCode> linkCodes = new IdentityHashMap<>();
      valBind.pat.visit(pat -> {
        if (pat instanceof Ast.IdPat) {
          final Ast.IdPat idPat = (Ast.IdPat) pat;
          final Type paramType = typeMap.getType(pat);
          final LinkCode linkCode = new LinkCode();
          linkCodes.put(idPat, linkCode);
          bindings.add(Binding.of(idPat.name, paramType, linkCode));
        }
      });
      code0 = compile(cx.bindAll(bindings), valBind.e);
      link(linkCodes, valBind.pat, code0);
    } else {
      code0 = compile(cx.bindAll(bindings), valBind.e);
    }
    final Code code;
    if (code0 instanceof CalciteCompiler.RelCode) {
      final CalciteCompiler.RelCode relCode = (CalciteCompiler.RelCode) code0;
      final Environment env = cx.bindAll(bindings).env;
      final RelNode rel = ((CalciteCompiler) this).toRel(env, valBind.e);
      final DataContext dataContext = Schemas.createDataContext(null, null);
      final Interpreter interpreter = new Interpreter(dataContext, rel);
      final Type type = typeMap.getType(valBind.e);
      final Function<Enumerable<Object[]>, List<Object>> converter =
          Converters.fromEnumerable(rel, type);
      code = new Code() {
        @Override public Describer describe(Describer describer) {
          return describer.start("calcite", d ->
              d.arg("plan", RelOptUtil.toString(rel)));
        }

        @Override public Object eval(EvalEnv evalEnv) {
          return converter.apply(interpreter);
        }
      };
    } else {
      code = code0;
    }
    newBindings.clear();
    final ImmutableList<Pair<Ast.Pat, Code>> patCodes =
        ImmutableList.of(Pair.of(valBind.pat, code));
    varCodes.add(new MatchCode(patCodes));

    if (actions != null) {
      final String name = ((Ast.IdPat) valBind.pat).name;
      final Type type0 = typeMap.getType(valBind.e);
      final Type type = typeMap.typeSystem.ensureClosed(type0);
      actions.add((output, outBindings, evalEnv) -> {
        final StringBuilder buf = new StringBuilder();
        try {
          final Object o = code.eval(evalEnv);
          outBindings.add(Binding.of(name, type, o));
          Pretty.pretty(buf, type, new Pretty.TypedVal(name, o, type0));
        } catch (Codes.MorelRuntimeException e) {
          e.describeTo(buf);
        }
        final String out = buf.toString();
        final Session session = (Session) evalEnv.getOpt(EvalEnv.SESSION);
        session.code = code;
        session.out = out;
        output.add(out);
      });
    }
  }

  private void link(Map<Ast.IdPat, LinkCode> linkCodes, Ast.Pat pat,
      Code code) {
    if (pat instanceof Ast.IdPat) {
      final LinkCode linkCode = linkCodes.get(pat);
      if (linkCode != null) {
        linkCode.refCode = code; // link the reference to the definition
      }
    } else if (pat instanceof Ast.TuplePat) {
      if (code instanceof Codes.TupleCode) {
        // Recurse into the tuple, binding names to code in parallel
        final List<Code> codes = ((Codes.TupleCode) code).codes;
        final List<Ast.Pat> pats = ((Ast.TuplePat) pat).args;
        Pair.forEach(codes, pats, (code1, pat1) ->
            link(linkCodes, pat1, code1));
      }
    }
  }

  /** A piece of code that is references another piece of code.
   * It is useful when defining recursive functions.
   * The reference is mutable, and is fixed up when the
   * function has been compiled.
   */
  private static class LinkCode implements Code {
    private Code refCode;

    @Override public Describer describe(Describer describer) {
      return describer.start("link", d -> {
        if (false) {
          // Don't recurse into refCode... or we'll never get out alive.
          d.arg("refCode", refCode);
        }
      });
    }

    public Object eval(EvalEnv env) {
      assert refCode != null; // link should have completed by now
      return refCode.eval(env);
    }
  }

  /** Code that implements {@link Compiler#compileMatchList(Context, List)}. */
  private static class MatchCode implements Code {
    private final ImmutableList<Pair<Ast.Pat, Code>> patCodes;

    MatchCode(ImmutableList<Pair<Ast.Pat, Code>> patCodes) {
      this.patCodes = patCodes;
    }

    @Override public Describer describe(Describer describer) {
      return describer.start("match", d ->
          patCodes.forEach(p ->
              d.arg("", p.left.toString()).arg("", p.right)));
    }

    @Override public Object eval(EvalEnv evalEnv) {
      return new Closure(evalEnv, patCodes);
    }
  }
}

// End Compiler.java
