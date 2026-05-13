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

package com.xonai.spark.sql.parser.trees

import com.xonai.utils.Utils

import scala.collection.mutable

/**
 * [[org.apache.spark.sql.catalyst.trees.TreeNodeTag]].
 */
case class TreeNodeTag[T](name: String)

/**
 * [[org.apache.spark.sql.catalyst.trees.TreeNode]].
 */
abstract class TreeNode[BaseType <: TreeNode[BaseType]] extends Product { self: BaseType =>

  private val tags: mutable.Map[TreeNodeTag[_], Any] = mutable.Map.empty

  def nodeName: String = Utils.getSimpleName(this.getClass)

  def copyTagsFrom(other: BaseType): Unit = {
    if (tags.isEmpty) {
      tags ++= other.tags
    }
  }

  def setTagValue[T](tag: TreeNodeTag[T], value: T): Unit = {
    tags(tag) = value
  }

  def getTagValue[T](tag: TreeNodeTag[T]): Option[T] = {
    tags.get(tag).map(_.asInstanceOf[T])
  }

  def unsetTagValue[T](tag: TreeNodeTag[T]): Unit = {
    tags -= tag
  }

  def children: IndexedSeq[BaseType]

  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
  }

  def find(f: BaseType => Boolean): Option[BaseType] = {
    if (f(this)) {
      Some(this)
    } else {
      children.foldLeft(Option.empty[BaseType]) { (l, r) => l.orElse(r.find(f)) }
    }
  }

  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  def foreachUp(f: BaseType => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  def map[A](f: BaseType => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret += f(_))
    ret.toSeq
  }

  def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret.+=))
    ret.toSeq
  }

  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }

  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = rule.applyOrElse(this, identity[BaseType])

    if (this fastEquals afterRule) {
      val rewritten_plan = mapChildren(_.transformDown(rule))
      if (this eq rewritten_plan) {
        this
      } else {
        rewritten_plan
      }
    } else {
      afterRule.copyTagsFrom(this)
      afterRule.mapChildren(_.transformDown(rule))
    }
  }

  def mapChildren(f: BaseType => BaseType): BaseType = {
    if (children.nonEmpty) {
      withNewChildren(children.map(f))
    } else {
      this
    }
  }

  final def withNewChildren(newChildren: IndexedSeq[BaseType]): BaseType = {
    assert(newChildren.size == children.size, "Incorrect number of children")
    if (children.isEmpty || childrenFastEquals(newChildren, children)) {
      this
    } else {
      val res = withNewChildrenInternal(newChildren)
      res.copyTagsFrom(this)
      res
    }
  }

  def withNewChildrenInternal(newChildren: IndexedSeq[BaseType]): BaseType

  private def childrenFastEquals(
      originalChildren: IndexedSeq[BaseType],
      newChildren: IndexedSeq[BaseType]
  ): Boolean = {
    val size = originalChildren.size
    var i = 0
    while (i < size) {
      if (!originalChildren(i).fastEquals(newChildren(i))) return false
      i += 1
    }
    true
  }
}

/**
 * Represents a node which the parser is unable to match.
 */
trait UnknownNode

/**
 * [[org.apache.spark.sql.catalyst.trees.LeafLike]].
 */
trait LeafLike[T <: TreeNode[T]] { self: TreeNode[T] =>

  override final def children: IndexedSeq[T] = IndexedSeq.empty

  override final def mapChildren(f: T => T): T = this.asInstanceOf[T]

  override def withNewChildrenInternal(newChildren: IndexedSeq[T]): T = this.asInstanceOf[T]
}

/**
 * [[org.apache.spark.sql.catalyst.trees.UnaryLike]].
 */
trait UnaryLike[T <: TreeNode[T]] { self: TreeNode[T] =>

  def child: T

  @transient
  override final lazy val children: IndexedSeq[T] = IndexedSeq(child)

  override final def mapChildren(f: T => T): T = {
    val newChild = f(child)
    if (newChild fastEquals child) {
      this.asInstanceOf[T]
    } else {
      val res = withNewChildInternal(newChild)
      res.copyTagsFrom(this.asInstanceOf[T])
      res
    }
  }

  override final def withNewChildrenInternal(newChildren: IndexedSeq[T]): T = {
    assert(newChildren.size == 1, "Incorrect number of children")
    withNewChildInternal(newChildren.head)
  }

  def withNewChildInternal(newChild: T): T
}

/**
 * [[org.apache.spark.sql.catalyst.trees.BinaryLike]].
 */
trait BinaryLike[T <: TreeNode[T]] { self: TreeNode[T] =>

  def left: T

  def right: T

  @transient
  override final lazy val children: IndexedSeq[T] = IndexedSeq(left, right)

  override final def mapChildren(f: T => T): T = {
    var newLeft = f(left)
    newLeft = if (newLeft fastEquals left) left else newLeft
    var newRight = f(right)
    newRight = if (newRight fastEquals right) right else newRight

    if (newLeft.eq(left) && newRight.eq(right)) {
      this.asInstanceOf[T]
    } else {
      val res = withNewChildrenInternal(newLeft, newRight)
      res.copyTagsFrom(this.asInstanceOf[T])
      res
    }
  }

  override final def withNewChildrenInternal(newChildren: IndexedSeq[T]): T = {
    assert(newChildren.size == 2, "Incorrect number of children")
    withNewChildrenInternal(newChildren(0), newChildren(1))
  }

  def withNewChildrenInternal(newLeft: T, newRight: T): T
}

/**
 * [[org.apache.spark.sql.catalyst.trees.TernaryLike]].
 */
trait TernaryLike[T <: TreeNode[T]] { self: TreeNode[T] =>

  def first: T

  def second: T

  def third: T

  @transient
  override final lazy val children: IndexedSeq[T] = IndexedSeq(first, second, third)

  override final def mapChildren(f: T => T): T = {
    var newFirst = f(first)
    newFirst = if (newFirst fastEquals first) first else newFirst
    var newSecond = f(second)
    newSecond = if (newSecond fastEquals second) second else newSecond
    var newThird = f(third)
    newThird = if (newThird fastEquals third) third else newThird

    if (newFirst.eq(first) && newSecond.eq(second) && newThird.eq(third)) {
      this.asInstanceOf[T]
    } else {
      val res = withNewChildrenInternal(newFirst, newSecond, newThird)
      res.copyTagsFrom(this.asInstanceOf[T])
      res
    }
  }

  override final def withNewChildrenInternal(newChildren: IndexedSeq[T]): T = {
    assert(newChildren.size == 3, "Incorrect number of children")
    withNewChildrenInternal(newChildren(0), newChildren(1), newChildren(2))
  }

  def withNewChildrenInternal(newFirst: T, newSecond: T, newThird: T): T
}

/**
 * [[org.apache.spark.sql.catalyst.trees.QuaternaryLike]].
 */
trait QuaternaryLike[T <: TreeNode[T]] { self: TreeNode[T] =>

  def first: T

  def second: T

  def third: T

  def fourth: T

  @transient
  override final lazy val children: IndexedSeq[T] = IndexedSeq(first, second, third, fourth)

  override final def mapChildren(f: T => T): T = {
    var newFirst = f(first)
    newFirst = if (newFirst fastEquals first) first else newFirst
    var newSecond = f(second)
    newSecond = if (newSecond fastEquals second) second else newSecond
    var newThird = f(third)
    newThird = if (newThird fastEquals third) third else newThird
    var newFourth = f(fourth)
    newFourth = if (newFourth fastEquals fourth) fourth else newFourth

    if (newFirst.eq(first) && newSecond.eq(second) && newThird.eq(third) && newFourth.eq(fourth)) {
      this.asInstanceOf[T]
    } else {
      val res = withNewChildrenInternal(newFirst, newSecond, newThird, newFourth)
      res.copyTagsFrom(this.asInstanceOf[T])
      res
    }
  }

  override final def withNewChildrenInternal(newChildren: IndexedSeq[T]): T = {
    assert(newChildren.size == 4, "Incorrect number of children")
    withNewChildrenInternal(newChildren(0), newChildren(1), newChildren(2), newChildren(3))
  }

  def withNewChildrenInternal(newFirst: T, newSecond: T, newThird: T, newFourth: T): T
}
