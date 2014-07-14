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

import akka.actor.{Actor, ActorLogging, Props}
import io.github.cloudify.scala.aws.kinesis.Client.ImplicitExecution._
import io.github.cloudify.scala.aws.kinesis.Definitions.Stream
import io.github.cloudify.scala.aws.kinesis.KinesisDsl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class StreamFactory extends Actor with ActorLogging {

  override def receive: Receive = {
    case Init(config) =>
      implicit val client = config.kinesis
      import config.streamName

      // create the actor with will actually publish to kinesis
      val producer = context.actorOf(Props[Producer], "producer")
      producer ! Init(config)

      // create or return the stream named streamName
      def mkStream(streams: Iterable[String]): Future[Stream] = {
        val exists = streams.exists(_ == streamName)
        if (!exists) {
          log.info(s"Stream $streamName doesn't exist.")
          Kinesis.streams.create(streamName): Future[Stream]
        } else {
          log.info(s"Stream $streamName exists.")
          Future.successful(streamName: Stream)
        }
      }

      // kick off a future which finds the current streams and creates or returns one named streamName
      val future = for {
        streamList <- Kinesis.streams.list
        stream <- mkStream(streamList)
      } yield stream

      // once we have the string, start scheduling periodic calls to the producer actor to publish messages
      future.onComplete {
        case Success(stream) =>
          context.system.scheduler.schedule(config.periodMs, config.periodMs, producer, Produce(stream))
        case Failure(error) =>
          log.error(s"Could not get hold of stream $streamName: $error")
      }
  }
}
