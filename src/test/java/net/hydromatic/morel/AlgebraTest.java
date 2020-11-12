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
package net.hydromatic.morel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.hydromatic.morel.ast.Ast;
import net.hydromatic.morel.ast.AstNode;
import net.hydromatic.morel.parse.ParseException;
import net.hydromatic.morel.type.TypeVar;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static net.hydromatic.morel.Ml.assertError;
import static net.hydromatic.morel.Ml.ml;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests translation of Morel programs to Apache Calcite relational algebra.
 */
public class AlgebraTest
{
  /** Tests a program that uses an external collection from the "scott" JDBC
   * database. */
  @Test public void testScott() {
    final String ml = "from e in scott.emps yield #deptno e\n";
    ml(ml)
        .withBinding("scott", DataSet.SCOTT.foreignValue())
        .assertType("int list")
        .assertEvalIter(
            equalsOrdered(20, 30, 30, 20, 30, 30, 10, 20, 10, 30, 20, 30, 20,
                10));
  }

  @Test public void testScottJoin() {
    final String ml = "let\n"
        + "  val emps = #emp scott\n"
        + "  and depts = #dept scott\n"
        + "in\n"
        + "  from e in emps, d in depts\n"
        + "    where #deptno e = #deptno d\n"
        + "    andalso #empno e >= 7900\n"
        + "    yield {empno = #empno e, dname = #dname d}\n"
        + "end\n";
    ml(ml)
        .withBinding("scott", DataSet.SCOTT.foreignValue())
        .assertType("{dname:string, empno:int} list")
        .assertEvalIter(
            equalsOrdered(list("SALES", 7900), list("RESEARCH", 7902),
                list("ACCOUNTING", 7934)));
  }

  /** As {@link #testScottJoin()} but without intermediate variables. */
  @Test public void testScottJoin2() {
    final String ml = "from e in #emp scott, d in #dept scott\n"
        + "  where #deptno e = #deptno d\n"
        + "  andalso #empno e >= 7900\n"
        + "  yield {empno = #empno e, dname = #dname d}\n";
    ml(ml)
        .withBinding("scott", DataSet.SCOTT.foreignValue())
        .assertType("{dname:string, empno:int} list")
        .assertEvalIter(
            equalsOrdered(list("SALES", 7900), list("RESEARCH", 7902),
                list("ACCOUNTING", 7934)));
  }

  /** As {@link #testScottJoin2()} but using dot notation ('e.field' rather
   * than '#field e'). */
  @Test public void testScottJoin2Dot() {
    final String ml = "from e in scott.emp, d in scott.dept\n"
        + "  where e.deptno = d.deptno\n"
        + "  andalso e.empno >= 7900\n"
        + "  yield {empno = e.empno, dname = d.dname}\n";
    ml(ml)
        .withBinding("scott", DataSet.SCOTT.foreignValue())
        .assertType("{dname:string, empno:int} list")
        .assertEvalIter(
            equalsOrdered(list("SALES", 7900), list("RESEARCH", 7902),
                list("ACCOUNTING", 7934)));
  }

  @Test public void testError() {
    ml("fn x y => x + y")
        .assertError(
            "Error: non-constructor applied to argument in pattern: x");
    ml("- case {a=1,b=2,c=3} of {a=x,b=y} => x + y")
        .assertError("Error: case object and rules do not agree [tycon "
            + "mismatch]\n"
            + "  rule domain: {a:[+ ty], b:[+ ty]}\n"
            + "  object: {a:[int ty], b:[int ty], c:[int ty]}\n"
            + "  in expression:\n"
            + "    (case {a=1,b=2,c=3}\n"
            + "      of {a=x,b=y} => x + y)\n");
    ml("fun f {a=x,b=y,...} = x+y")
        .assertError("Error: unresolved flex record (need to know the names of "
            + "ALL the fields\n"
            + " in this context)\n"
            + "  type: {a:[+ ty], b:[+ ty]; 'Z}\n");
    ml("fun f {a=x,...} = x | {b=y,...} = y;")
        .assertError("stdIn:1.24-1.33 Error: can't find function arguments in "
            + "clause\n"
            + "stdIn:1.24-1.33 Error: illegal function symbol in clause\n"
            + "stdIn:1.6-1.37 Error: clauses do not all have same function "
            + "name\n"
            + "stdIn:1.36 Error: unbound variable or constructor: y\n"
            + "stdIn:1.2-1.37 Error: unresolved flex record\n"
            + "   (can't tell what fields there are besides #a)\n");
    ml("fun f {a=x,...} = x | f {b=y,...} = y")
        .assertError("Error: unresolved flex record (need to know the names of "
            + "ALL the fields\n"
            + " in this context)\n"
            + "  type: {a:'Y, b:'Y; 'Z}\n");
    ml("fun f {a=x,...} = x\n"
        + "  | f {b=y,...} = y\n"
        + "  | f {a=x,b=y,c=z} = x+y+z")
        .assertError("stdIn:1.6-3.20 Error: match redundant\n"
            + "          {a=x,b=_,c=_} => ...\n"
            + "    -->   {a=_,b=y,c=_} => ...\n"
            + "    -->   {a=x,b=y,c=z} => ...\n");
    ml("fun f 1 = 1 | f n = n * f (n - 1) | g 2 = 2")
        .assertError("stdIn:3.5-3.46 Error: clauses don't all have same "
            + "function name");
  }

}

// End MainTest.java
