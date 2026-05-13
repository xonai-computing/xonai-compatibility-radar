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

package com.xonai.spark.sql.support

import com.xonai.spark.sql.parser.trees.{TreeNode, TreeNodeTag}

/**
 * Represents whether an [[com.xonai.spark.sql.parser.expressions.Expression]] or a
 * [[com.xonai.spark.sql.parser.plans.SparkPlan]] is supported by some execution engine.
 */
case class NodeSupport(unsupportedReasons: Set[String]) {

  def isSupported: Boolean = unsupportedReasons.isEmpty

  def +(other: NodeSupport): NodeSupport = {
    if (this.isSupported) {
      other
    } else if (other.isSupported) {
      this
    } else {
      NodeSupport(unsupportedReasons ++ other.unsupportedReasons)
    }
  }
}

object NodeSupport {

  private val IsolatedTag = new TreeNodeTag[NodeSupport]("IsolatedNodeSupport")
  private val ComposedTag = new TreeNodeTag[NodeSupport]("ComposedNodeSupport")

  val Supported = NodeSupport(Set.empty)

  val Unknown = NodeSupport(Set("Unknown"))

  val NotImplemented = NodeSupport(Set("NotImplemented"))

  val UndefinedDataType = NodeSupport(Set("UndefinedDataType"))

  val UnsupportedDataType = NodeSupport(Set("UnsupportedDataType"))

  def setIsolatedSupport(node: TreeNode[_], support: NodeSupport): Unit = {
    node.setTagValue(IsolatedTag, support)
  }

  def getIsolatedSupport(node: TreeNode[_]): NodeSupport = {
    node.getTagValue(IsolatedTag).get
  }

  def setComposedSupport(node: TreeNode[_], support: NodeSupport): Unit = {
    node.setTagValue(ComposedTag, support)
  }

  def getComposedSupport(node: TreeNode[_]): NodeSupport = {
    node.getTagValue(ComposedTag).get
  }
}
