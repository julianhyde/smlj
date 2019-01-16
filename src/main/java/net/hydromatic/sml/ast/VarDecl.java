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
package net.hydromatic.sml.ast;

import java.util.Objects;

/** Parse tree node of a variable declaration. */
public class VarDecl extends AstNode {
  public final Id var;
  public final AstNode expression;

  /** Creates an Id. */
  public VarDecl(Pos pos, Id var, AstNode expression) {
    super(pos);
    this.var = Objects.requireNonNull(var);
    this.expression = Objects.requireNonNull(expression);
  }

  @Override public int hashCode() {
    return Objects.hash(var, expression);
  }

  @Override public boolean equals(Object o) {
    return o == this
        || o instanceof VarDecl
        && this.var.equals(((VarDecl) o).var)
        && this.expression.equals(((VarDecl) o).expression);
  }

  @Override public String toString() {
    return "val " + var + " = " + expression;
  }
}

// End VarDecl.java
