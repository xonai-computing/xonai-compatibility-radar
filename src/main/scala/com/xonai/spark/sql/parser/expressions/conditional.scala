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

package com.xonai.spark.sql.parser.expressions

import com.xonai.spark.sql.parser.types.{BooleanType, DataType}

/**
 * [[org.apache.spark.sql.catalyst.expressions.If]].
 */
case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
    extends TernaryExpression {

  override def first: Expression = predicate

  override def second: Expression = trueValue

  override def third: Expression = falseValue

  override lazy val dataType: DataType = {
    trueValue.dataType.intersect(falseValue.dataType)
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    withNewChildren(
      IndexedSeq(
        predicate.resolveDataType(BooleanType),
        trueValue.resolveDataType(newDataType),
        falseValue.resolveDataType(newDataType)
      )
    )
  }

  override def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression
  ): Expression = {
    copy(predicate = newFirst, trueValue = newSecond, falseValue = newThird)
  }
}

/**
 * [[org.apache.spark.sql.catalyst.expressions.CaseWhen]].
 */
case class CaseWhen(branches: IndexedSeq[(Expression, Expression)], elseValue: Option[Expression])
    extends Expression {

  override lazy val dataType: DataType = {
    val branchesDataType = branches.map(_._2.dataType).reduce(DataType.intersection)

    elseValue.fold(branchesDataType) { expr =>
      branchesDataType.intersect(expr.dataType)
    }
  }

  override def children: IndexedSeq[Expression] = {
    branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue.toIndexedSeq
  }

  override def resolveDataType(outputType: DataType): Expression = {
    val newDataType = outputType.intersect(dataType)
    val newBranches = branches.flatMap { branch =>
      branch._1.resolveDataType(BooleanType) ::
        branch._2.resolveDataType(newDataType) :: Nil
    }
    val newElse = elseValue.map(_.resolveDataType(newDataType))
    withNewChildren(newBranches ++ newElse.toIndexedSeq)
  }

  override def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    val branchCount = newChildren.length / 2
    val newBranches = (0 until branchCount).map { i =>
      (newChildren.apply(2 * i), newChildren.apply(2 * i + 1))
    }
    val newElseValue = if (elseValue.isDefined) Some(newChildren.last) else None
    copy(branches = newBranches, elseValue = newElseValue)
  }
}
