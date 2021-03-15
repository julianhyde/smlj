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

import org.junit.Test;

import java.util.stream.Stream;

import static net.hydromatic.morel.Matchers.equalsOrdered;
import static net.hydromatic.morel.Matchers.list;
import static net.hydromatic.morel.Ml.ml;

import static org.hamcrest.core.Is.is;

/**
 * Tests translation of Morel programs to Apache Calcite relational algebra.
 */
public class AlgebraTest {
  /** Tests a program that uses an external collection from the "scott" JDBC
   * database. */
  @Test public void testScott() {
    final String ml = "let\n"
        + "  val emps = #emp scott\n"
        + "in\n"
        + "  from e in emps yield #deptno e\n"
        + "end\n";
    ml(ml)
        .withBinding("scott", BuiltInDataSet.SCOTT)
        .assertType("int list")
        .assertEvalIter(
            equalsOrdered(20, 30, 30, 20, 30, 30, 10, 20, 10, 30, 20, 30, 20,
                10));
  }

  /** As previous, but with more concise syntax. */
  @Test public void testScott2() {
    final String ml = "from e in scott.emp yield e.deptno";
    final String plan = "LogicalProject(deptno=[$7])\n"
        + "  JdbcTableScan(table=[[scott, EMP]])\n";
    ml(ml)
        .withBinding("scott", BuiltInDataSet.SCOTT)
        .assertType("int list")
        .assertCalcite(is(plan))
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
        .withBinding("scott", BuiltInDataSet.SCOTT)
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
        .withBinding("scott", BuiltInDataSet.SCOTT)
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
        .withBinding("scott", BuiltInDataSet.SCOTT)
        .assertType("{dname:string, empno:int} list")
        .assertEvalIter(
            equalsOrdered(list("SALES", 7900), list("RESEARCH", 7902),
                list("ACCOUNTING", 7934)));
  }

  /** Tests that Morel gives the same answer with and without Calcite. */
  @Test public void testQueryList() {
    final String[] queries = {
        "from",
        "from e in scott.emp",
        "from e in scott.emp yield e.deptno",
        "from e in scott.emp yield {e.deptno, e.ename}",
        "from e in scott.emp yield {e.ename, e.deptno}",
        "from e in scott.emp\n"
            + "  yield {e.ename, x = e.deptno + e.empno, b = true, "
            // + "c = #\"c\", "
            + "i = 3, r = 3.14, "
            // + "u = (), "
            + "s = \"hello\"}",
        // "from e in scott.emp yield ()",
        "from e in scott.emp yield e",
        "from e in scott.emp where e.job = \"CLERK\" yield e",
        "from n in [1,2,3] yield n",
        "from n in [1,2,3] where n mod 2 = 1 andalso n < 3 yield n",
        "from n in [1,2,3] where false yield n",
        "from n in [1,2,3] where n < 2 orelse n > 2 yield n * 3",
        "from r in [{a=1,b=2},{a=1,b=0},{a=2,b=1}]\n"
            + "  order r.a desc, r.b\n"
            + "  yield {r.a, b10 = r.b * 10}",
        "from r in [{a=2,b=3},{a=2,b=1},{a=1,b=1}]\n"
            + "  group r.a",
        "from r in [{a=2,b=3},{a=2,b=1},{a=1,b=1}]\n"
            + "  group compute sb = sum of r.b",
        "from r in [{a=2,b=3},{a=2,b=1},{a=1,b=1}]\n"
            + "  group r.a\n"
            + "  yield a",
        "from r in [{a=2,b=3}]\n"
            + "group r.b compute sb = sum of r.b,\n"
            + "    mb = min of r.b, a = count",
        "from r in [{a=2,b=3}]\n"
            + "group r.b compute sb = sum of r.b,\n"
            + "    mb = min of r.b, a = count\n"
            + "yield {a, a2 = a + b, sb}",
        "from e in scott.emp,\n"
            + "  d in scott.dept\n"
            + "where e.deptno = d.deptno\n"
            + "yield {e.ename, d.dname}",
        "from e in scott.emp,\n"
            + "  d in scott.dept\n"
            + "where e.deptno = d.deptno",
        "from e in scott.emp,\n"
            + "  d in scott.dept\n"
            + "where e.deptno = d.deptno\n"
            + "yield e",
        "from e in scott.emp,\n"
            + "  d in scott.dept\n"
            + "where e.deptno = d.deptno\n"
            + "andalso e.job = \"CLERK\"\n"
            + "yield d",
        "from e in scott.emp,\n"
            + "  d in scott.dept\n"
            + "where e.deptno = d.deptno\n"
            + "andalso e.job = \"CLERK\"\n"
            + "group e.mgr",
        "from e in scott.emp,\n"
            + "  g in scott.salgrade\n"
            + "where e.sal >= g.losal\n"
            + "  andalso e.sal < g.hisal",
        "from e in scott.emp,\n"
            + "  d in scott.dept,"
            + "  g in scott.salgrade\n"
            + "where e.sal >= g.losal\n"
            + "  andalso e.sal < g.hisal\n"
            + "  andalso d.deptno = e.deptno",
        "from e in scott.emp,\n"
            + "  d in scott.dept,"
            + "  g in scott.salgrade\n"
            + "where e.sal >= g.losal\n"
            + "  andalso e.sal < g.hisal\n"
            + "  andalso d.deptno = e.deptno\n"
            + "group g.grade compute c = count",
        "[1, 2, 3] union [2, 3, 4]",
        "[10, 15, 20] union (from d in scott.dept yield d.deptno)",
        "[10, 15, 20] except (from d in scott.dept yield d.deptno)",
        "[10, 15, 20] intersect (from d in scott.dept yield d.deptno)",

        // the following 4 are equivalent
        "from e in scott.emp where e.deptno = 30 yield e.empno",
        "#let\n"
            + "  val emps = #emp scott\n"
            + "in\n"
            + "  from e in emps\n"
            + "  where e.deptno = 30\n"
            + "  yield e.empno\n"
            + "end",
        "#let\n"
            + "  val emps = #emp scott\n"
            + "  val thirty = 30\n"
            + "in\n"
            + "  from e in emps\n"
            + "  where e.deptno = thirty\n"
            + "  yield e.empno\n"
            + "end",
        "#map (fn e => (#empno e))\n"
            + "  (filter (fn e => (#deptno e) = 30) (#emp scott))",
    };
    Stream.of(queries).filter(q -> !q.startsWith("#")).forEach(query -> {
      try {
        checkEqual(query);
      } catch (AssertionError | RuntimeException e) {
        throw new RuntimeException("during query [" + query + "]", e);
      }
    });
  }

  /** Translates a hybrid expression. The leaf cannot be translated to Calcite
   * and therefore becomes a Morel table function; the root can. */
  @Test public void testNative() {
    String query = ""
        + "from r in\n"
        + "  List.tabulate (6, fn i =>\n"
        + "    {i, j = i + 3, s = String.substring (\"morel\", 0, i)})\n"
        + "yield {r.j, r.s}";
    checkEqual(query);
  }

  void checkEqual(String ml) {
    ml(ml).withBinding("scott", BuiltInDataSet.SCOTT).assertEvalSame();
  }
}

// End AlgebraTest.java
