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

// Top-level declarations in standard ML

// -----------------------------------------------------------------------------
// Top-level types
eqtype unit; // defined in structure General
eqtype int; // defined in structure Int
eqtype word; // defined in structure Word
type real; // defined in structure Real
eqtype char; // defined in structure Char
eqtype string; // defined in structure String
type substring; // defined in structure Substring
type exn; // defined in structure General
eqtype 'a array; // defined in structure Array
eqtype 'a vector; // defined in structure Vector
eqtype 'a ref; // primitive
datatype bool =
    false
  | true; // primitive
datatype 'a option =
    NONE
  | SOME of 'a; // defined in structure Option
datatype order = LESS | EQUAL | GREATER; // defined in structure General
datatype 'a list =
    nil
  | :: of ('a * 'a list); // primitive

// -----------------------------------------------------------------------------
// The structure General defines exceptions, datatypes, and functions
// which are used throughout the SML Basis Library, and are useful in a
// wide range of programs.
//
// All of the types and values defined in General are available
// unqualified at the top-level.

//signature GENERAL
//structure General :> GENERAL
//
//eqtype unit
//type exn = exn
//
//exception Bind
//exception Match
//exception Chr
//exception Div
//exception Domain
//exception Fail of string
//exception Overflow
//exception Size
//exception Span
//exception Subscript
//
//val exnName : exn -> string
//val exnMessage : exn -> string
//
//datatype order = LESS | EQUAL | GREATER
//val ! : 'a ref -> 'a
//val := : 'a ref * 'a -> unit
//val o : ('b -> 'c) * ('a -> 'b) -> 'a -> 'c
//val before : 'a * unit -> 'a
//val ignore : 'a -> unit
// -----------------------------------------------------------------------------

// Top-level functions
val ! : 'a ref -> 'a; // bound to General.!
val := : 'a ref * 'a -> unit; // bound to General.:=
val @ : ('a list * 'a list) -> 'a list; // bound to List.@
val ^ : string * string -> string; // bound to String.^
val app : ('a -> unit) -> 'a list -> unit; // bound to List.app
val before : 'a * unit -> 'a; // bound to General.before
val ceil : real -> int; // bound to Real.ceil
val chr : int -> char; // bound to Char.chr
val concat : string list -> string; // bound to String.concat
val exnMessage : exn -> string; // bound to General.exnMessage
val exnName : exn -> string; // bound to General.exnName
val explode : string -> char list; // bound to String.explode
val floor : real -> int; // bound to Real.floor
val foldl : ('a*'b->'b)-> 'b -> 'a list -> 'b; // bound to List.foldl
val foldr : ('a*'b->'b)-> 'b -> 'a list -> 'b; // bound to List.foldr
val getOpt : ('a option * 'a) -> 'a; // bound to Option.getOpt
val hd : 'a list -> 'a; // bound to List.hd
val ignore : 'a -> unit; // bound to General.ignore
val implode : char list -> string; // bound to String.implode
val isSome : 'a option -> bool; // bound to Option.isSome
val length : 'a list -> int; // bound to List.length
val map : ('a -> 'b) -> 'a list -> 'b list; // bound to List.map
val not : bool -> bool; // bound to Bool.not
val null : 'a list -> bool; // bound to List.null
val o : ('a->'b) * ('c->'a) -> 'c->'b; // bound to General.o
val ord : char -> int; // bound to Char.ord
val print : string -> unit; // bound to TextIO.print
val real : int -> real; // bound to Real.fromInt
val ref : 'a -> 'a ref; // bound to primitive
val rev : 'a list -> 'a list; // bound to List.rev
val round : real -> int; // bound to Real.round
val size : string -> int; // bound to String.size
val str : char -> string; // bound to String.str
val substring : string * int * int -> string; // bound to String.substring
val tl : 'a list -> 'a list; // bound to List.tl
val trunc : real -> int; // bound to Real.trunc
val use : string -> unit; // implementation dependent
val valOf : 'a option -> 'a; // bound to Option.valOf
val vector : 'a list -> 'a vector; // bound to Vector.fromList

// Infix identifiers
infix  7  * / div mod
infix  6  + - ^
infixr 5  :: @
infix  4  = <> > >= < <=
infix  3  := o
infix  0  before

// End top.sml
