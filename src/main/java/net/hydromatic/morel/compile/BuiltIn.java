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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import net.hydromatic.morel.type.Binding;
import net.hydromatic.morel.type.DataType;
import net.hydromatic.morel.type.DummyType;
import net.hydromatic.morel.type.PrimitiveType;
import net.hydromatic.morel.type.RecordType;
import net.hydromatic.morel.type.Type;
import net.hydromatic.morel.type.TypeSystem;
import net.hydromatic.morel.type.TypeVar;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static net.hydromatic.morel.type.PrimitiveType.BOOL;
import static net.hydromatic.morel.type.PrimitiveType.CHAR;
import static net.hydromatic.morel.type.PrimitiveType.INT;
import static net.hydromatic.morel.type.PrimitiveType.STRING;
import static net.hydromatic.morel.type.PrimitiveType.UNIT;

/** Built-in constants and functions. */
public enum BuiltIn {
  /** Literal "true", of type "bool". */
  TRUE(null, "true", ts -> BOOL),

  /** Literal "false", of type "bool". */
  FALSE(null, "false", ts -> BOOL),

  /** Function "not", of type "bool &rarr; bool". */
  NOT(null, "not", ts -> ts.fnType(BOOL, BOOL)),

  /** Function "abs", of type "int &rarr; int". */
  ABS(null, "abs", ts -> ts.fnType(INT, INT)),

  /** Infix operator "^", of type "string * string &rarr; string". */
  OP_CARET(null, "op ^", ts -> ts.fnType(ts.tupleType(STRING, STRING), STRING)),

  /** Infix operator "except", of type "&alpha; list * &alpha; list &rarr;
   * &alpha; list". */
  OP_EXCEPT(null, "op except", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(ts.listType(h.get(0)), ts.listType(h.get(0))),
              ts.listType(h.get(0))))),

  /** Infix operator "intersect", of type "&alpha; list * &alpha; list &rarr;
   * &alpha; list". */
  OP_INTERSECT(null, "op intersect", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(ts.listType(h.get(0)), ts.listType(h.get(0))),
              ts.listType(h.get(0))))),

  /** Infix operator "union", of type "&alpha; list * &alpha; list &rarr;
   * &alpha; list". */
  OP_UNION(null, "op union", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(ts.listType(h.get(0)), ts.listType(h.get(0))),
              ts.listType(h.get(0))))),

  /** Infix operator "::" (list cons), of type
   * "&alpha; * &alpha; list &rarr; &alpha; list". */
  OP_CONS(null, "op ::", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), ts.listType(h.get(0))),
              ts.listType(h.get(0))))),

  /** Infix operator "div", of type "int * int &rarr; int". */
  OP_DIV(null, "op div", ts -> ts.fnType(ts.tupleType(INT, INT), INT)),

  /** Infix operator "/", of type "&alpha; * &alpha; &rarr; &alpha;"
   * (where &alpha; must be numeric). */
  OP_DIVIDE(null, "op /", PrimitiveType.INT, ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), h.get(0)), h.get(0)))),

  /** Infix operator "=", of type "&alpha; * &alpha; &rarr; bool". */
  OP_EQ(null, "op =", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), h.get(0)), BOOL))),

  /** Infix operator "&ge;", of type "&alpha; * &alpha; &rarr; bool"
   * (where &alpha; must be comparable). */
  OP_GE(null, "op >=", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), h.get(0)), BOOL))),

  /** Infix operator "&gt;", of type "&alpha; * &alpha; &rarr; bool"
   * (where &alpha; must be comparable). */
  OP_GT(null, "op >", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), h.get(0)), BOOL))),

  /** Infix operator "&le;", of type "&alpha; * &alpha; &rarr; bool"
   * (where &alpha; must be comparable). */
  OP_LE(null, "op <=", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), h.get(0)), BOOL))),

  /** Infix operator "&lt;", of type "&alpha; * &alpha; &rarr; bool"
   * (where &alpha; must be comparable). */
  OP_LT(null, "op <", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), h.get(0)), BOOL))),

  /** Infix operator "&lt;&gt;", of type "&alpha; * &alpha; &rarr; bool". */
  OP_NE(null, "op <>", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), h.get(0)), BOOL))),

  /** Infix operator "elem", of type "&alpha; * &alpha; list; &rarr; bool". */
  OP_ELEM(null, "op elem", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), ts.listType(h.get(0))), BOOL))),

  /** Infix operator "notElem", of type "&alpha; * &alpha; list; &rarr;
   * bool". */
  OP_NOT_ELEM(null, "op notElem", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), ts.listType(h.get(0))), BOOL))),

  /** Infix operator "-", of type "&alpha; * &alpha; &rarr; &alpha;"
   * (where &alpha; must be numeric). */
  OP_MINUS(null, "op -", PrimitiveType.INT, ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), h.get(0)), h.get(0)))),

  /** Infix operator "mod", of type "int * int &rarr; int". */
  OP_MOD(null, "op mod", ts -> ts.fnType(ts.tupleType(INT, INT), INT)),

  /** Infix operator "+", of type "&alpha; * &alpha; &rarr; &alpha;"
   * (where &alpha; must be numeric). */
  OP_PLUS(null, "op +", PrimitiveType.INT, ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), h.get(0)), h.get(0)))),

  /** Prefix operator "~", of type "&alpha; &rarr; &alpha;"
   * (where &alpha; must be numeric). */
  OP_NEGATE(null, "op ~", ts ->
      ts.forallType(1, h -> ts.fnType(h.get(0), h.get(0)))),

  /** Infix operator "-", of type "&alpha; * &alpha; &rarr; &alpha;"
   * (where &alpha; must be numeric). */
  OP_TIMES(null, "op *", PrimitiveType.INT, ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.get(0), h.get(0)), h.get(0)))),

  /** Function "General.ignore", of type "&alpha; &rarr; unit". */
  IGNORE("General", "ignore", "ignore", ts ->
      ts.forallType(1, h -> ts.fnType(h.get(0), UNIT))),

  /** Operator "General.op o", of type "(&beta; &rarr; &gamma;) *
   * (&alpha; &rarr; &beta;) &rarr; &alpha; &rarr; &gamma;"
   *
   * <p>"f o g" is the function composition of "f" and "g". Thus, "(f o g) a"
   * is equivalent to "f (g a)". */
  GENERAL_OP_O("General", "op o", "op o", ts ->
      ts.forallType(3, h ->
          ts.fnType(
              ts.tupleType(ts.fnType(h.get(1), h.get(2)),
                  ts.fnType(h.get(0), h.get(1))),
              ts.fnType(h.get(0), h.get(2))))),

  /** Constant "String.maxSize", of type "int".
   *
   * <p>"The longest allowed size of a string". */
  STRING_MAX_SIZE("String", "maxSize", ts -> INT),

  /** Function "String.size", of type "string &rarr; int".
   *
   * <p>"size s" returns |s|, the number of characters in string s. */
  STRING_SIZE("String", "size", ts -> ts.fnType(STRING, INT)),

  /** Function "String.sub", of type "string * int &rarr; char".
   *
   * <p>"sub (s, i)" returns the {@code i}<sup>th</sup> character of s, counting
   * from zero. This raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SUBSCRIPT Subscript}
   * if i &lt; 0 or |s| &le; i. */
  STRING_SUB("String", "sub", ts -> ts.fnType(ts.tupleType(STRING, INT), CHAR)),

  /** Function "String.extract", of type "string * int * int option &rarr;
   * string".
   *
   * <p>"extract (s, i, NONE)" and "extract (s, i, SOME j)" return substrings of
   * {@code s}. The first returns the substring of {@code s} from the
   * {@code i}<sup>th</sup> character to the end of the string, i.e., the string
   * {@code s[i..|s|-1]}. This raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SUBSCRIPT Subscript}
   * if {@code i < 0} or {@code |s| < i}.
   *
   * <p>The second form returns the substring of size {@code j} starting at
   * index {@code i}, i.e., the {@code string s[i..i+j-1]}. It raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SUBSCRIPT Subscript}
   * if {@code i < 0} or {@code j < 0} or {@code |s| < i + j}. Note that, if
   * defined, extract returns the empty string when {@code i = |s|}. */
  STRING_EXTRACT("String", "extract", ts ->
      ts.fnType(ts.tupleType(STRING, INT, ts.option(INT)), STRING)),

  /** Function "String.substring", of type "string * int * int &rarr; string".
   *
   * <p>"substring (s, i, j)" returns the substring s[i..i+j-1], i.e., the
   * substring of size j starting at index i. This is equivalent to
   * extract(s, i, SOME j). */
  STRING_SUBSTRING("String", "substring", ts ->
      ts.fnType(ts.tupleType(STRING, INT, INT), STRING)),

  /** Function "String.concat", of type "string list &rarr; string".
   *
   * <p>"concat l" is the concatenation of all the strings in l. This raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SIZE Size}
   * if the sum of all the sizes is greater than maxSize.  */
  STRING_CONCAT("String", "concat", ts ->
      ts.fnType(ts.listType(STRING), STRING)),

  /** Function "String.concatWith", of type "string &rarr; string list &rarr;
   * string".
   *
   * <p>"concatWith s l" returns the concatenation of the strings in the list l
   * using the string s as a separator. This raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SIZE Size}
   * if the size of the resulting string would be greater than maxSize. */
  STRING_CONCAT_WITH("String", "concatWith", ts ->
      ts.fnType(STRING, ts.listType(STRING), STRING)),

  /** Function "String.str", of type "char &rarr; string".
   *
   * <p>"str c" is the string of size one containing the character c. */
  STRING_STR("String", "str", ts -> ts.fnType(CHAR, STRING)),

  /** Function "String.implode", of type "char list &rarr; string".
   *
   * <p>"implode l" generates the string containing the characters in the list
   * l. This is equivalent to {@code concat (List.map str l)}. This raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SIZE Size}
   * if the resulting string would have size greater than maxSize. */
  STRING_IMPLODE("String", "implode", ts ->
      ts.fnType(ts.listType(CHAR), STRING)),

  /** Function "String.explode", of type "string &rarr; char list".
   *
   * <p>"explode s" is the list of characters in the string s. */
  STRING_EXPLODE("String", "explode", ts ->
      ts.fnType(STRING, ts.listType(CHAR))),

  /** Function "String.map", of type "(char &rarr; char) &rarr; string
   * &rarr; string".
   *
   * <p>"map f s" applies f to each element of s from left to right, returning
   * the resulting string. It is equivalent to
   * {@code implode(List.map f (explode s))}.  */
  STRING_MAP("String", "map", ts ->
      ts.fnType(ts.fnType(CHAR, CHAR), STRING, STRING)),

  /** Function "String.translate", of type "(char &rarr; string) &rarr; string
   * &rarr; string".
   *
   * <p>"translate f s" returns the string generated from s by mapping each
   * character in s by f. It is equivalent to
   * {code concat(List.map f (explode s))}. */
  STRING_TRANSLATE("String", "translate", ts ->
      ts.fnType(ts.fnType(CHAR, STRING), STRING, STRING)),

  /** Function "String.isPrefix", of type "string &rarr; string &rarr; bool".
   *
   * <p>"isPrefix s1 s2" returns true if the string s1 is a prefix of the string
   * s2. Note that the empty string is a prefix of any string, and that a string
   * is a prefix of itself. */
  STRING_IS_PREFIX("String", "isPrefix", ts -> ts.fnType(STRING, STRING, BOOL)),

  /** Function "String.isSubstring", of type "string &rarr; string &rarr; bool".
   *
   * <p>"isSubstring s1 s2" returns true if the string s1 is a substring of the
   * string s2. Note that the empty string is a substring of any string, and
   * that a string is a substring of itself. */
  STRING_IS_SUBSTRING("String", "isSubstring", ts ->
      ts.fnType(STRING, STRING, BOOL)),

  /** Function "String.isSuffix", of type "string &rarr; string &rarr; bool".
   *
   * <p>"isSuffix s1 s2" returns true if the string s1 is a suffix of the string
   * s2. Note that the empty string is a suffix of any string, and that a string
   * is a suffix of itself. */
  STRING_IS_SUFFIX("String", "isSuffix", ts -> ts.fnType(STRING, STRING, BOOL)),

  /** Constant "List.nil", of type "&alpha; list".
   *
   * <p>"nil" is the empty list.
   */
  LIST_NIL("List", "nil", ts -> ts.forallType(1, h -> h.list(0))),

  /** Function "List.null", of type "&alpha; list &rarr; bool".
   *
   * <p>"null l" returns true if the list l is empty.
   */
  LIST_NULL("List", "null", ts ->
      ts.forallType(1, h -> ts.fnType(h.list(0), BOOL))),

  /** Function "List.length", of type "&alpha; list &rarr; int".
   *
   * <p>"length l" returns the number of elements in the list l.
   */
  LIST_LENGTH("List", "length", ts ->
      ts.forallType(1, h -> ts.fnType(h.list(0), INT))),

  /** Function "List.at", of type "&alpha; list * &alpha; list &rarr; &alpha;
   * list".
   *
   * <p>"l1 @ l2" returns the list that is the concatenation of l1 and l2.
   */
  // TODO: remove
  LIST_AT("List", "at", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.list(0), h.list(0)), h.list(0)))),

  /** Operator "List.op @", of type "&alpha; list * &alpha; list &rarr; &alpha;
   * list".
   *
   * <p>"l1 @ l2" returns the list that is the concatenation of l1 and l2.
   */
  LIST_OP_AT("List", "op @", "op @", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.list(0), h.list(0)), h.list(0)))),

  /** Function "List.hd", of type "&alpha; list &rarr; &alpha;".
   *
   * <p>"hd l" returns the first element of l. It raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#EMPTY Empty}
   * if l is nil.
   */
  LIST_HD("List", "hd", ts ->
      ts.forallType(1, h -> ts.fnType(h.list(0), h.get(0)))),

  /** Function "List.tl", of type "&alpha; list &rarr; &alpha; list".
   *
   * <p>"tl l" returns all but the first element of l. It raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#EMPTY empty}
   * if l is nil.
   */
  LIST_TL("List", "tl", ts ->
      ts.forallType(1, h -> ts.fnType(h.list(0), h.list(0)))),

  /** Function "List.last", of type "&alpha; list &rarr; &alpha;".
   *
   * <p>"last l" returns the last element of l. It raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#EMPTY empty}
   * if l is nil.
   */
  LIST_LAST("List", "last", ts ->
      ts.forallType(1, h -> ts.fnType(h.list(0), h.get(0)))),

  /** Function "List.getItem", of type "&alpha; list &rarr;
   * (&alpha; * &alpha; list) option".
   *
   * <p>"getItem l" returns {@code NONE} if the list is empty, and
   * {@code SOME(hd l,tl l)} otherwise. This function is particularly useful for
   * creating value readers from lists of characters. For example,
   * {@code Int.scan StringCvt.DEC getItem} has the type
   * {@code (int, char list) StringCvt.reader}
   * and can be used to scan decimal integers from lists of characters.
   */
  LIST_GET_ITEM("List", "getItem", ts ->
      ts.forallType(1, h ->
          ts.fnType(h.list(0),
              ts.option(ts.tupleType(h.get(0), h.list(0)))))),

  /** Function "List.nth", of type "&alpha; list * int &rarr; &alpha;".
   *
   * <p>"nth (l, i)" returns the {@code i}<sup>th</sup> element of the list
   * {@code l}, counting from 0. It raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SUBSCRIPT Subscript}
   * if {@code i < 0} or {@code i >= length l}.
   * We have {@code nth(l,0) = hd l}, ignoring exceptions.
   */
  LIST_NTH("List", "nth", ts ->
      ts.forallType(1, h -> ts.fnType(ts.tupleType(h.list(0), INT), h.get(0)))),

  /** Function "List.take", of type "&alpha; list * int &rarr; &alpha; list".
   *
   * <p>"take (l, i)" returns the first i elements of the list l. It raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SUBSCRIPT Subscript}
   * if i &lt; 0 or i &gt; length l.
   * We have {@code take(l, length l) = l}.
   */
  LIST_TAKE("List", "take", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.list(0), INT), h.list(0)))),

  /** Function "List.drop", of type "&alpha; list * int &rarr; &alpha; list".
   *
   * <p>"drop (l, i)" returns what is left after dropping the first i elements
   * of the list l.
   *
   * <p>It raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SUBSCRIPT Subscript}
   * if i &lt; 0 or i &gt; length l.
   *
   * <p>It holds that
   * {@code take(l, i) @ drop(l, i) = l} when 0 &le; i &le; length l.
   *
   * <p>We also have {@code drop(l, length l) = []}.
   */
  LIST_DROP("List", "drop", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.list(0), INT), h.list(0)))),

  /** Function "List.rev", of type "&alpha; list &rarr; &alpha; list".
   *
   * <p>"rev l" returns a list consisting of l's elements in reverse order.
   */
  LIST_REV("List", "rev", ts ->
      ts.forallType(1, h -> ts.fnType(h.list(0), h.list(0)))),

  /** Function "List.concat", of type "&alpha; list list &rarr; &alpha; list".
   *
   * <p>"concat l" returns the list that is the concatenation of all the lists
   * in l in order.
   * {@code concat[l1,l2,...ln] = l1 @ l2 @ ... @ ln}
   */
  LIST_CONCAT("List", "concat", ts ->
      ts.forallType(1, h -> ts.fnType(ts.listType(h.list(0)), h.list(0)))),

  /** Function "List.revAppend", of type "&alpha; list * &alpha; list &rarr;
   * &alpha; list".
   *
   * <p>"revAppend (l1, l2)" returns (rev l1) @ l2.
   */
  LIST_REV_APPEND("List", "revAppend", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.list(0), h.list(0)), h.list(0)))),

  /** Function "List.app", of type "(&alpha; &rarr; unit) &rarr; &alpha; list
   * &rarr; unit".
   *
   * <p>"app f l" applies f to the elements of l, from left to right.
   */
  LIST_APP("List", "app", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.fnType(h.get(0), UNIT), h.list(0), UNIT))),

  /** Function "List.map", of type
   * "(&alpha; &rarr; &beta;) &rarr; &alpha; list &rarr; &beta; list".
   *
   * <p>"map f l" applies f to each element of l from left to right, returning
   * the list of results.
   */
  LIST_MAP("List", "map", "map", ts ->
      ts.forallType(2, t ->
          ts.fnType(ts.fnType(t.get(0), t.get(1)),
              ts.listType(t.get(0)), ts.listType(t.get(1))))),

  /** Function "List.mapPartial", of type
   * "(&alpha; &rarr; &beta; option) &rarr; &alpha; list &rarr; &beta; list".
   *
   * <p>"mapPartial f l" applies f to each element of l from left to right,
   * returning a list of results, with SOME stripped, where f was defined. f is
   * not defined for an element of l if f applied to the element returns NONE.
   * The above expression is equivalent to:
   * {@code ((map valOf) o (filter isSome) o (map f)) l}
   */
  LIST_MAP_PARTIAL("List", "mapPartial", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(h.get(0), h.option(1)), h.list(0), h.list(1)))),

  /** Function "List.find", of type "(&alpha; &rarr; bool) &rarr; &alpha; list
   * &rarr; &alpha; option".
   *
   * <p>"find f l" applies f to each element x of the list l, from left to
   * right, until {@code f x} evaluates to true. It returns SOME(x) if such an x
   * exists; otherwise it returns NONE.
   */
  LIST_FIND("List", "find", ts ->
      ts.forallType(1, h ->
          ts.fnType(h.predicate(0), h.list(0), h.option(0)))),

  /** Function "List.filter", of type
   * "(&alpha; &rarr; bool) &rarr; &alpha; list &rarr; &alpha; list".
   *
   * <p>"filter f l" applies f to each element x of l, from left to right, and
   * returns the list of those x for which {@code f x} evaluated to true, in the
   * same order as they occurred in the argument list.
   */
  LIST_FILTER("List", "filter", ts ->
      ts.forallType(1, h -> ts.fnType(h.predicate(0), h.list(0), h.list(0)))),

  /** Function "List.partition", of type "(&alpha; &rarr; bool) &rarr;
   * &alpha; list &rarr; &alpha; list * &alpha; list".
   *
   * <p>"partition f l" applies f to each element x of l, from left to right,
   * and returns a pair (pos, neg) where pos is the list of those x for which
   * {@code f x} evaluated to true, and neg is the list of those for which
   * {@code f x} evaluated to false. The elements of pos and neg retain the same
   * relative order they possessed in l.
   */
  LIST_PARTITION("List", "partition", ts ->
      ts.forallType(1, h ->
          ts.fnType(h.predicate(0), h.list(0),
              ts.tupleType(h.list(0), h.list(0))))),

  /** Function "List.foldl", of type "(&alpha; * &beta; &rarr; &beta;) &rarr;
   *  &beta; &rarr; &alpha; list &rarr; &beta;".
   *
   * <p>"foldl f init [x1, x2, ..., xn]" returns
   * {@code f(xn,...,f(x2, f(x1, init))...)}
   * or {@code init} if the list is empty.
   */
  LIST_FOLDL("List", "foldl", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(ts.tupleType(h.get(0), h.get(1)), h.get(1)),
              h.get(1), h.list(0), h.get(1)))),

  /** Function "List.foldr", of type "(&alpha; * &beta; &rarr; &beta;) &rarr;
   *  &beta; &rarr; &alpha; list &rarr; &beta;".
   *
   * <p>"foldr f init [x1, x2, ..., xn]" returns
   * {@code f(x1, f(x2, ..., f(xn, init)...))}
   * or {@code init} if the list is empty.
   */
  LIST_FOLDR("List", "foldr", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(ts.tupleType(h.get(0), h.get(1)), h.get(1)),
              h.get(1), h.list(0), h.get(1)))),

  /** Function "List.exists", of type "(&alpha; &rarr; bool) &rarr; &alpha; list
   * &rarr; bool".
   *
   * <p>"exists f l" applies f to each element x of the list l, from left to
   * right, until {@code f x} evaluates to true; it returns true if such an x
   * exists and false otherwise.
   */
  LIST_EXISTS("List", "exists", ts ->
      ts.forallType(1, h -> ts.fnType(h.predicate(0), h.list(0), BOOL))),

  /** Function "List.all", of type
   * "(&alpha; &rarr; bool) &rarr; &alpha; list &rarr; bool".
   *
   * <p>"all f l" applies f to each element x of the list l, from left to right,
   * until {@code f x} evaluates to false; it returns false if such an x exists
   * and true otherwise. It is equivalent to not(exists (not o f) l)).
   */
  LIST_ALL("List", "all", ts ->
      ts.forallType(1, h -> ts.fnType(h.predicate(0), h.list(0), BOOL))),

  /** Function "List.tabulate", of type
   * "int * (int &rarr; &alpha;) &rarr; &alpha; list".
   *
   * <p>"tabulate (n, f)" returns a list of length n equal to
   * {@code [f(0), f(1), ..., f(n-1)]}, created from left to right. It raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SIZE Size}
   * if n &lt; 0.
   */
  LIST_TABULATE("List", "tabulate", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(INT, ts.fnType(INT, h.get(0))), h.list(0)))),

  /** Function "List.collate", of type "(&alpha; * &alpha; &rarr; order)
   * &rarr; &alpha; list * &alpha; list &rarr; order".
   *
   * <p>"collate f (l1, l2)" performs lexicographic comparison of the two lists
   * using the given ordering f on the list elements.
   */
  LIST_COLLATE("List", "collate", ts ->
    ts.forallType(1, h ->
        ts.fnType(
            ts.fnType(ts.tupleType(h.get(0), h.get(0)), ts.lookup("order")),
            ts.tupleType(h.list(0), h.list(0)),
            ts.lookup("order")))),

  /** Function "Option.getOpt", of type
   * "&alpha; option * &alpha; &rarr; &alpha;".
   *
   * <p>{@code getOpt(opt, a)} returns v if opt is SOME(v); otherwise it
   * returns a. */
  OPTION_GET_OPT("Option", "getOpt", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.option(0), h.get(0)),
              h.get(0)))),

  /** Function "Option.isSome", of type
   * "&alpha; option &rarr; bool".
   *
   * <p>{@code isSome opt} returns true if opt is SOME(v); otherwise it returns
   * false. */
  OPTION_IS_SOME("Option", "isSome", ts ->
      ts.forallType(1, h -> ts.fnType(h.option(0), BOOL))),

  /** Function "Option.valOf", of type
   * "&alpha; option &rarr; &alpha;".
   *
   * <p>{@code valOf opt} returns v if opt is SOME(v); otherwise it raises
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#OPTION Option}. */
  OPTION_VAL_OF("Option", "valOf", ts ->
      ts.forallType(1, h -> ts.fnType(h.option(0), h.get(0)))),

  /** Function "Option.filter", of type
   * "(&alpha; &rarr; bool) &rarr; &alpha; &rarr; &alpha; option".
   *
   * <p>{@code filter f a} returns SOME(a) if f(a) is true and NONE
   * otherwise. */
  OPTION_FILTER("Option", "filter", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.fnType(h.get(0), BOOL), h.get(0), h.option(0)))),

  /** Function "Option.join", of type
   * "&alpha; option option &rarr; &alpha; option".
   *
   * <p>{@code join opt} maps NONE to NONE and SOME(v) to v.*/
  OPTION_JOIN("Option", "join", ts ->
      ts.forallType(1, h -> ts.fnType(ts.option(h.option(0)), h.option(0)))),

  /** Function "Option.app", of type
   * "(&alpha; &rarr; unit) &rarr; &alpha; option &rarr; unit".
   *
   * <p>{@code app f opt} applies the function f to the value v if opt is
   * SOME(v), and otherwise does nothing. */
  OPTION_APP("Option", "app", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.fnType(h.option(0), UNIT), h.option(0), UNIT))),

  /** Function "Option.map", of type
   * "(&alpha; &rarr; &beta;) &rarr; &alpha; option &rarr; &beta; option".
   *
   * <p>{@code map f opt} maps NONE to NONE and SOME(v) to SOME(f v). */
  OPTION_MAP("Option", "map", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(h.get(0), h.get(1)),
              h.option(0), h.option(1)))),

  /** Function "Option.mapPartial", of type
   * "(&alpha; &rarr; &beta; option) &rarr; &alpha; option &rarr; &beta;
   * option".
   *
   * <p>{@code mapPartial f opt} maps NONE to NONE and SOME(v) to f (v). */
  OPTION_MAP_PARTIAL("Option", "mapPartial", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(h.get(0), h.option(1)),
              h.option(0), h.option(1)))),

  /** Function "Option.compose", of type
   * "(&alpha; &rarr; &beta;) * (&gamma; &rarr; &alpha; option)
   * &rarr; &gamma; &rarr; &beta; option".
   *
   * <p>{@code compose (f, g) a} returns NONE if g(a) is NONE; otherwise, if
   * g(a) is SOME(v), it returns SOME(f v).
   *
   * <p>Thus, the compose function composes {@code f} with the partial
   * function {@code g} to produce another partial function. The expression
   * compose {@code (f, g)} is equivalent to {@code (map f) o g}. */
  OPTION_COMPOSE("Option", "compose", ts ->
      ts.forallType(3, h ->
          ts.fnType(
              ts.tupleType(ts.fnType(h.get(0), h.get(1)),
                  ts.fnType(h.get(2), h.option(0))),
              h.get(2), h.option(1)))),

  /** Function "Option.composePartial", of type
   * "(&alpha; &rarr; &beta; option) * (&gamma; &rarr; &alpha; option)
   * &rarr; &gamma; &rarr; &beta; option".
   *
   * <p>{@code composePartial (f, g) a} returns NONE if g(a) is NONE; otherwise,
   * if g(a) is SOME(v), it returns f(v).
   *
   * <p>Thus, the {@code composePartial} function composes the two partial
   * functions {@code f} and {@code g} to produce another partial function.
   * The expression {@code composePartial (f, g)} is equivalent to
   * {@code (mapPartial f) o g}. */
  OPTION_COMPOSE_PARTIAL("Option", "composePartial", ts ->
      ts.forallType(3, h ->
          ts.fnType(
              ts.tupleType(ts.fnType(h.get(0), h.option(1)),
                  ts.fnType(h.get(2), h.option(0))),
              h.get(2), h.option(1)))),

  /** Function "Relational.count", aka "count", of type "int list &rarr; int".
   *
   * <p>Often used with {@code group}:
   *
   * <blockquote>
   *   <pre>
   *     from e in emps
   *     group deptno = (#deptno e)
   *       compute sumId = sum of (#id e)
   *   </pre>
   * </blockquote>
   */
  RELATIONAL_COUNT("Relational", "count", "count", ts ->
      ts.forallType(1, h -> ts.fnType(h.list(0), INT))),

  /** Function "Relational.sum", aka "sum", of type
   *  "&alpha; list &rarr; &alpha;" (where &alpha; must be numeric).
   *
   * <p>Often used with {@code group}:
   *
   * <blockquote>
   *   <pre>
   *     from e in emps
   *     group deptno = (#deptno e)
   *       compute sumId = sum of (#id e)
   *   </pre>
   * </blockquote>
   */
  RELATIONAL_SUM("Relational", "sum", "sum", ts ->
      ts.forallType(1, h -> ts.fnType(ts.listType(h.get(0)), h.get(0)))),

  /** Function "Relational.max", aka "max", of type
   *  "&alpha; list &rarr; &alpha;" (where &alpha; must be comparable). */
  RELATIONAL_MAX("Relational", "max", "max", ts ->
      ts.forallType(1, h -> ts.fnType(ts.listType(h.get(0)), h.get(0)))),

  /** Function "Relational.min", aka "min", of type
   *  "&alpha; list &rarr; &alpha;" (where &alpha; must be comparable). */
  RELATIONAL_MIN("Relational", "min", "min", ts ->
      ts.forallType(1, h -> ts.fnType(ts.listType(h.get(0)), h.get(0)))),

  /** Function "Sys.env", aka "env", of type "unit &rarr; string list". */
  SYS_ENV("Sys", "env", "env", ts ->
      ts.fnType(UNIT, ts.listType(ts.tupleType(STRING, STRING)))),

  /** Function "Sys.plan", aka "plan", of type "unit &rarr; string". */
  SYS_PLAN("Sys", "plan", "plan", ts -> ts.fnType(UNIT, STRING)),

  /** Function "Sys.set", aka "set", of type "string * &alpha; &rarr; unit". */
  SYS_SET("Sys", "set", "set", ts ->
      ts.forallType(1, h -> ts.fnType(ts.tupleType(STRING, h.get(0)), UNIT))),

  /** Function "Sys.show", aka "set", of type "string &rarr; string option". */
  SYS_SHOW("Sys", "show", "show", ts -> ts.fnType(STRING, ts.option(STRING))),

  /** Function "Sys.unset", aka "unset", of type "string &rarr; unit". */
  SYS_UNSET("Sys", "unset", "unset", ts -> ts.fnType(STRING, UNIT)),

  /** Constant "Vector.maxLen" of type "int".
   *
   * <p>The maximum length of vectors supported by this implementation. Attempts
   * to create larger vectors will result in the
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SIZE Size}
   * exception being raised. */
  VECTOR_MAX_LEN("Vector", "maxLen", ts -> INT),

  /** Function "Vector.fromList" of type "&alpha; list &rarr; &alpha; vector".
   *
   * <p>{@code fromList l} creates a new vector from {@code l}, whose length is
   * {@code length l} and with the {@code i}<sup>th</sup> element of {@code l}
   * used as the {@code i}<sup>th</sup> element of the vector. If the length of
   * the list is greater than {@code maxLen}, then the
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SIZE Size}
   * exception is raised. */
  VECTOR_FROM_LIST("Vector", "fromList", ts ->
      ts.forallType(1, h -> ts.fnType(h.list(0), h.vector(0)))),

  /** Function "Vector.tabulate" of type
   * "int * (int &rarr; &alpha;) &rarr; &alpha; vector".
   *
   * <p>{@code tabulate (n, f)} creates a vector of {@code n} elements, where
   * the elements are defined in order of increasing index by applying {@code f}
   * to the element's index. This is equivalent to the expression:
   *
   * <blockquote>{@code fromList (List.tabulate (n, f))}</blockquote>
   *
   * <p>If {@code n < 0} or {@code maxLen < n}, then the
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SIZE Size}
   * exception is raised. */
  VECTOR_TABULATE("Vector", "tabulate", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(INT, ts.fnType(INT, h.get(0))), h.vector(0)))),

  /** Function "Vector.length" of type "&alpha; vector &rarr; int".
   *
   * <p>{@code length vec} returns {@code |vec|}, the length of the vector
   * {@code vec}. */
  VECTOR_LENGTH("Vector", "length", ts ->
      ts.forallType(1, h -> ts.fnType(h.vector(0), INT))),

  /** Function "Vector.sub" of type "&alpha; vector * int &rarr; &alpha;".
   *
   * <p>{@code sub (vec, i)} returns the {@code i}<sup>th</sup> element of the
   * vector {@code vec}. If {@code i < 0} or {@code |vec| <= i}, then the
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SUBSCRIPT Subscript}
   * exception is raised. */
  VECTOR_SUB("Vector", "sub", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.vector(0), INT), h.get(0)))),

  /** Function "Vector.update" of type
   * "&alpha; vector * int * &alpha; &rarr; &alpha; vector".
   *
   * <p>{@code update (vec, i, x)} returns a new vector, identical to
   * {@code vec}, except the {@code i}<sup>th</sup> element of {@code vec} is
   * set to {@code x}. If {@code i < 0} or {@code |vec| <= i}, then the
   * {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SUBSCRIPT Subscript}
   * exception is raised. */
  VECTOR_UPDATE("Vector", "update", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.tupleType(h.vector(0), INT, h.get(0)), h.vector(0)))),

  /** Function "Vector.concat" of type
   * "&alpha; vector list &rarr; &alpha; vector".
   *
   * <p>{@code concat l}
   * returns the vector that is the concatenation of the vectors in the list
   * {@code l}. If the total length of these vectors exceeds {@code maxLen},
   * then the {@link net.hydromatic.morel.eval.Codes.BuiltInExn#SIZE Size}
   * exception is raised. */
  VECTOR_CONCAT("Vector", "concat", ts ->
      ts.forallType(1, h -> ts.fnType(ts.listType(h.vector(0)), h.vector(0)))),

  /** Function "Vector.appi" of type
   * "(int * &alpha; &rarr; unit) &rarr; &alpha; vector &rarr; unit".
   *
   * <p>{@code appi f vec} applies the function {@code f} to the elements of
   * a vector in left to right order (i.e., in order of increasing indices).
   * The {@code appi} function is more general than {@code app}, and supplies
   * both the element and the element's index to the function {@code f}.
   * Equivalent to:
   *
   * <blockquote>
   *   {@code List.app f (foldri (fn (i,a,l) => (i,a)::l) [] vec)}
   * </blockquote> */
  VECTOR_APPI("Vector", "appi", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.fnType(ts.tupleType(INT, h.get(0)), UNIT),
              h.vector(0), UNIT))),

  /** Function "Vector.app" of type
   * "(&alpha; &rarr; unit) &rarr; &alpha; vector &rarr; unit".
   *
   * <p>{@code app f vec} applies the function {@code f} to the elements of
   * a vector in left to right order (i.e., in order of increasing indices).
   * Equivalent to:
   *
   * <blockquote>
   *   {@code List.app f (foldr (fn (a,l) => a::l) [] vec)}
   * </blockquote> */
  VECTOR_APP("Vector", "app", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.fnType(h.get(0), UNIT), h.vector(0), UNIT))),

  /** Function "Vector.mapi" of type "(int * &alpha; &rarr; &beta;) &rarr;
   * &alpha; vector &rarr; &beta; vector".
   *
   * <p>{@code mapi f vec} produces a new vector by mapping the function
   * {@code f} from left to right over the argument vector. The form
   * {@code mapi} is more general, and supplies {@code f} with the vector
   * index of an element along with the element. Equivalent to:
   *
   * <blockquote>
   * {@code fromList (List.map f (foldri (fn (i,a,l) => (i,a)::l) [] vec))}
   * </blockquote> */
  VECTOR_MAPI("Vector", "mapi", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(ts.tupleType(INT, h.get(0)), h.get(1)),
              h.vector(0), h.vector(1)))),

  /** Function "Vector.map" of type "(&alpha; &rarr; &beta;) &rarr;
   * &alpha; vector &rarr; &beta; vector".
   *
   * <p>{@code map f vec} produces a new vector by mapping the function
   * {@code f} from left to right over the argument vector. Equivalent to:
   *
   * <blockquote>
   * {@code fromList (List.map f (foldr (fn (a,l) => a::l) [] vec))}
   * </blockquote> */
  VECTOR_MAP("Vector", "map", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(h.get(0), h.get(1)), h.vector(0), h.vector(1)))),

  /** Function "Vector.foldli" of type "(int * &alpha; * &beta; &rarr; &beta;)
   * &rarr; &beta; &rarr; &alpha; vector &rarr; &beta;".
   *
   * <p>{@code foldli f init vec} folds the function {@code f} over all the
   * elements of a vector, using the value {@code init} as the initial value.
   * Applies the function {@code f} from left to right (increasing indices).
   * The functions {@code foldli} and {@code foldri} are more general, and
   * supply both the element and the element's index to the function f. */
  VECTOR_FOLDLI("Vector", "foldli", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(ts.tupleType(INT, h.get(0), h.get(1)), h.get(1)),
              h.get(1), h.vector(0), h.get(1)))),

  /** Function "Vector.foldri" of type "(int * &alpha; * &beta; &rarr; &beta;)
   * &rarr; &beta; &rarr; &alpha; vector &rarr; &beta;".
   *
   * <p>{@code foldri f init vec} folds the function {@code f} over all the
   * elements of a vector, using the value {@code init} as the initial value.
   * Applies the function {@code f} from right to left (decreasing indices).
   * The functions {@code foldli} and {@code foldri} are more general, and
   * supply both the element and the element's index to the function f. */
  VECTOR_FOLDRI("Vector", "foldri", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(ts.tupleType(INT, h.get(0), h.get(1)), h.get(1)),
              h.get(1), h.vector(0), h.get(1)))),

  /** Function "Vector.foldl" of type "(&alpha; * &beta; &rarr; &beta;) &rarr;
   * &beta; &rarr; &alpha; vector &rarr; &beta;".
   *
   * <p>{@code foldl f init vec} folds the function {@code f} over all the
   * elements of a vector, using the value {@code init} as the initial value.
   * Applies the function {@code f} from left to right (increasing indices).
   * Equivalent to
   *
   * <blockquote>
   *   {@code foldli (fn (_, a, x) => f(a, x)) init vec}
   * </blockquote> */
  VECTOR_FOLDL("Vector", "foldl", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(ts.tupleType(h.get(0), h.get(1)), h.get(1)),
              h.get(1), h.vector(0), h.get(1)))),

  /** Function "Vector.foldr" of type "(&alpha; * &beta; &rarr; &beta;) &rarr;
   * &beta; &rarr; &alpha; vector &rarr; &beta;".
   *
   * <p>{@code foldr f init vec} folds the function {@code f} over all the
   * elements of a vector, using the value {@code init} as the initial value.
   * Applies the function {@code f} from right to left (decreasing indices).
   * Equivalent to
   *
   * <blockquote>
   *   {@code foldri (fn (_, a, x) => f(a, x)) init vec}
   * </blockquote> */
  VECTOR_FOLDR("Vector", "foldr", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(ts.tupleType(h.get(0), h.get(1)), h.get(1)),
              h.get(1), h.vector(0), h.get(1)))),

  /** Function "Vector.findi" of type "(int * &alpha; &rarr; bool) &rarr;
   * &alpha; vector &rarr; (int * &alpha;) option".
   *
   * <p>{@code findi f vec} applies {@code f} to each element of the vector
   * {@code vec}, from left to right (i.e., increasing indices), until a
   * {@code true} value is returned. If this occurs, the function returns the
   * element; otherwise, it return {@code NONE}. The function {@code findi} is
   * more general than {@code find}, and also supplies {@code f} with the vector
   * index of the element and, upon finding an entry satisfying the predicate,
   * returns that index with the element. */
  VECTOR_FINDI("Vector", "findi", ts ->
      ts.forallType(2, h ->
          ts.fnType(ts.fnType(ts.tupleType(INT, h.get(0)), BOOL),
              h.vector(0), ts.option(ts.tupleType(INT, h.get(0)))))),

  /** Function "Vector.find" of type
   * "(&alpha; &rarr; bool) &rarr; &alpha; vector &rarr; &alpha; option".
   *
   * <p>{@code find f vec} applies {@code f} to each element of the vector
   * {@code vec}, from left to right (i.e., increasing indices), until a
   * {@code true} value is returned. If this occurs, the function returns the
   * element; otherwise, it returns {@code NONE}. */
  VECTOR_FIND("Vector", "find", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.fnType(h.get(0), BOOL), h.vector(0), h.option(0)))),

  /** Function "Vector.exists" of type
   * "(&alpha; &rarr; bool) &rarr; &alpha; vector &rarr; bool".
   *
   * <p>{@code exists f vec} applies {@code f} to each element {@code x} of the
   * vector {@code vec}, from left to right (i.e., increasing indices), until
   * {@code f(x)} evaluates to {@code true}; it returns {@code true} if such
   * an {@code x} exists and {@code false} otherwise. */
  VECTOR_EXISTS("Vector", "exists", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.fnType(h.get(0), BOOL), h.vector(0), BOOL))),

  /** Function "Vector.all" of type
   * "(&alpha; &rarr; bool) &rarr; &alpha; vector &rarr; bool".
   *
   * <p>{@code all f vec} applies {@code f} to each element {@code x} of the
   * vector {@code vec}, from left to right (i.e., increasing indices), until
   * {@code f(x)} evaluates to {@code false}; it returns {@code false} if such
   * an {@code x} exists and {@code true} otherwise. It is equivalent to
   * {@code not (exists (not o f ) vec))}. */
  VECTOR_ALL("Vector", "all", ts ->
      ts.forallType(1, h ->
          ts.fnType(ts.fnType(h.get(0), BOOL), h.vector(0), BOOL))),

  /** Function "Vector.collate" of type "(&alpha; * &alpha; &rarr; order) &rarr;
   * &alpha; vector * &alpha; vector &rarr; order".
   *
   * <p>{@code collate f (v1, v2)} performs lexicographic comparison of the two
   * vectors using the given ordering {@code f} on elements. */
  VECTOR_COLLATE("Vector", "collate", ts ->
      ts.forallType(1, h ->
          ts.fnType(
              ts.fnType(ts.tupleType(h.get(0), h.get(0)), ts.lookup("order")),
              ts.tupleType(h.vector(0), h.vector(0)),
              ts.lookup("order"))));

  /** Name of the structure (e.g. "List", "String"), or null. */
  public final String structure;

  /** Unqualified name, e.g. "map" (for "List.map") or "true". */
  public final String mlName;

  /** An alias, or null. For example, "List.map" has an alias "map". */
  public final String alias;

  /** Derives a type, in a particular type system, for this constant or
   * function. */
  public final Function<TypeSystem, Type> typeFunction;

  private final PrimitiveType preferredType;

  public static final ImmutableMap<String, BuiltIn> BY_ML_NAME;

  public static final SortedMap<String, Structure> BY_STRUCTURE;

  static {
    ImmutableMap.Builder<String, BuiltIn> byMlName = ImmutableMap.builder();
    final SortedMap<String, ImmutableSortedMap.Builder<String, BuiltIn>> map =
        new TreeMap<>();
    for (BuiltIn builtIn : values()) {
      if (builtIn.alias != null) {
        byMlName.put(builtIn.alias, builtIn);
      }
      if (builtIn.structure == null) {
        byMlName.put(builtIn.mlName, builtIn);
      } else {
        map.compute(builtIn.structure, (name, mapBuilder) -> {
          if (mapBuilder == null) {
            mapBuilder = ImmutableSortedMap.naturalOrder();
          }
          return mapBuilder.put(builtIn.mlName, builtIn);
        });
      }
    }
    BY_ML_NAME = byMlName.build();
    final ImmutableSortedMap.Builder<String, Structure> b =
        ImmutableSortedMap.naturalOrder();
    map.forEach((structure, mapBuilder) ->
        b.put(structure, new Structure(structure, mapBuilder.build())));
    BY_STRUCTURE = b.build();
  }

  BuiltIn(@Nullable String structure, String mlName,
      Function<TypeSystem, Type> typeFunction) {
    this(structure, mlName, null, typeFunction, null);
  }

  BuiltIn(@Nullable String structure, String mlName,
      @Nonnull PrimitiveType preferredType,
      Function<TypeSystem, Type> typeFunction) {
    this(structure, mlName, null, typeFunction, preferredType);
  }

  BuiltIn(@Nullable String structure, String mlName,
      @Nullable String alias, Function<TypeSystem, Type> typeFunction) {
    this(structure, mlName, alias, typeFunction, null);
  }

  BuiltIn(@Nullable String structure, String mlName,
      @Nullable String alias, Function<TypeSystem, Type> typeFunction,
      @Nullable PrimitiveType preferredType) {
    this.structure = structure;
    this.mlName = Objects.requireNonNull(mlName);
    this.alias = alias;
    this.typeFunction = Objects.requireNonNull(typeFunction);
    this.preferredType = preferredType;
  }

  /** Calls a consumer once per value. */
  public static void forEach(TypeSystem typeSystem,
      BiConsumer<BuiltIn, Type> consumer) {
    for (BuiltIn builtIn : values()) {
      final Type type = builtIn.typeFunction.apply(typeSystem);
      consumer.accept(builtIn, type);
    }
  }

  /** Calls a consumer once per structure. */
  public static void forEachStructure(TypeSystem typeSystem,
      BiConsumer<Structure, Type> consumer) {
    final TreeMap<String, Type> nameTypes = new TreeMap<>(RecordType.ORDERING);
    BY_STRUCTURE.values().forEach(structure -> {
      nameTypes.clear();
      structure.memberMap.forEach((name, builtIn) ->
          nameTypes.put(name, builtIn.typeFunction.apply(typeSystem)));
      consumer.accept(structure, typeSystem.recordType(nameTypes));
    });
  }

  /** Defines built-in {@code datatype} and {@code eqtype} instances, e.g.
   *  {@code option}, {@code vector}. */
  public static void dataTypes(TypeSystem ts, List<Binding> bindings) {
    defineDataType(ts, bindings, "order", 0, h ->
        h.tyCon("LESS").tyCon("EQUAL").tyCon("GREATER"));
    defineDataType(ts, bindings, "option", 1, h ->
        h.tyCon("NONE").tyCon("SOME", h.get(0)));
    defineEqType(ts, "vector", 1);
  }

  private static void defineEqType(TypeSystem ts, String name, int varCount) {
    defineDataType(ts, new ArrayList<>(), name, varCount, h -> h);
  }

  private static void defineDataType(TypeSystem ts, List<Binding> bindings,
      String name, int varCount, UnaryOperator<DataTypeHelper> transform) {
    final List<TypeVar> tyVars = new ArrayList<>();
    for (int i = 0; i < varCount; i++) {
      tyVars.add(ts.typeVariable(i));
    }
    final Map<String, Type> tyCons = new LinkedHashMap<>();
    transform.apply(new DataTypeHelper() {
      public DataTypeHelper tyCon(String name, Type type) {
        tyCons.put(name, type);
        return this;
      }

      public DataTypeHelper tyCon(String name) {
        return tyCon(name, DummyType.INSTANCE);
      }

      public TypeVar get(int i) {
        return tyVars.get(i);
      }
    });
    final DataType dataType = ts.dataType(name, tyVars, tyCons);
    tyCons.keySet().forEach(tyConName ->
        bindings.add(ts.bindTyCon(dataType, tyConName)));
  }

  /** Callback used when defining a datatype. */
  private interface DataTypeHelper {
    DataTypeHelper tyCon(String name);
    DataTypeHelper tyCon(String name, Type type);
    TypeVar get(int i);
  }

  /** Built-in structure. */
  public static class Structure {
    public final String name;
    public final SortedMap<String, BuiltIn> memberMap;

    Structure(String name, SortedMap<String, BuiltIn> memberMap) {
      this.name = Objects.requireNonNull(name);
      this.memberMap = ImmutableSortedMap.copyOf(memberMap);
    }
  }

  public void prefer(Consumer<PrimitiveType> consumer) {
    if (preferredType != null) {
      consumer.accept(preferredType);
    }
  }
}

// End BuiltIn.java
