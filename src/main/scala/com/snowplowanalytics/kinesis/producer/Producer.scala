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

import java.nio.ByteBuffer

import akka.actor.{Actor, ActorLogging}
import io.github.cloudify.scala.aws.kinesis.Client
import io.github.cloudify.scala.aws.kinesis.Client.ImplicitExecution._
import io.github.cloudify.scala.aws.kinesis.Definitions.Stream

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

case class Init(config: ProducerConfig)
case class Produce(stream: Stream)

class Producer extends Actor with ActorLogging {
  var client: Client = _
  override def receive: Receive = {
    case Init(config) =>
      client = config.kinesis
    case Produce(stream) =>
      implicit val client = this.client
      val timestamp = System.currentTimeMillis()
      log.info(s"Writing String record.")
      val stringData = s"example-record-$timestamp"
      val stringKey = s"partition-key-${timestamp % 100000}"
      log.info(s"  + data: $stringData")
      log.info(s"  + key: $stringKey")
      stream.put(ByteBuffer.wrap(stringData.getBytes), stringKey).andThen {
        case Success(result) =>
          log.info(s"Writing successful.")
          log.info(s"  + ShardId: ${result.shardId}")
          log.info(s"  + SequenceNumber: ${result.sequenceNumber}")
        case Failure(error) =>
          log.error(s"Failed writing $stringKey: $stringData to $stream: $error")
      }
  }
}
