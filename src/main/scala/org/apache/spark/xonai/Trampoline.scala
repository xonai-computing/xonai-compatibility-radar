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

package org.apache.spark.xonai

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.status.KVUtils
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStoreView

object Trampoline {

  def kvUtilsViewToSeq[T](view: KVStoreView[T]): Seq[T] = {
    KVUtils.viewToSeq(view, Int.MaxValue)(_ => true)
  }

  def hadoopUtilCreateFile(fs: FileSystem, path: Path, allowEC: Boolean): FSDataOutputStream = {
    SparkHadoopUtil.createFile(fs, path, allowEC)
  }

  def utilsGetSimpleName(cls: Class[_]): String = {
    Utils.getSimpleName(cls)
  }

  def utilsStripDollars(s: String): String = {
    Utils.stripDollars(s)
  }
}
