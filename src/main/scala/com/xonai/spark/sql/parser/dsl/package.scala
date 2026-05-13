/*
 * Copyright 2026 XONAI LTD
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xonai.spark.sql.parser

import com.xonai.spark.sql.parser.expressions.{EqualTo, Expression, GreaterThan, LessThan, Literal}

import scala.language.implicitConversions

/**
 * [[org.apache.spark.sql.catalyst.dsl]].
 */
package object dsl {

  trait ImplicitOperators {

    def expr: Expression

    def <(other: Expression): LessThan = LessThan(expr, other)
    def >(other: Expression): GreaterThan = GreaterThan(expr, other)
    def ===(other: Expression): EqualTo = EqualTo(expr, other)
  }

  trait ExpressionConversions {
    implicit class DslExpression(e: Expression) extends ImplicitOperators {
      override def expr: Expression = e
    }

    implicit def stringToLiteral(s: String): Literal = Literal(s)

    implicit class DslString(val s: String) extends ImplicitOperators {
      override def expr: Expression = Literal(s)
    }
  }

  object expressions extends ExpressionConversions
}
