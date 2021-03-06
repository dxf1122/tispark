/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.spark

import java.util.Properties

import com.typesafe.scalalogging.slf4j.LazyLogging

object TestFramework extends LazyLogging {
  val ConfName = "tispark_config.properties"

  def main(args: Array[String]): Unit = {

    val prop: Properties = loadConf(ConfName)
    new TestCase(prop).init()
    System.exit(0)
  }

  def loadConf(conf: String): Properties = {
    val confStream = getClass.getClassLoader.getResourceAsStream(conf)
    val prop = new Properties()
    prop.load(confStream)
    prop
  }
}
