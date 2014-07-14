/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.kinesis.producer

import akka.actor._
import com.typesafe.config.ConfigFactory

object ProducerApp extends App {

  val config = ConfigFactory.parseResources("default.conf")

  val producerConfig = new ProducerConfig(config.getConfig("producer"))
  val sys = ActorSystem("kinesis-producer")
  val creator = sys.actorOf(Props[StreamFactory], "factory")

  creator ! Init(producerConfig)


  // block forever
  this.synchronized {
    this.wait()
  }
}
