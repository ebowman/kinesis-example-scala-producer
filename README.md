# Sample Scala Event Producer for Amazon Kinesis

Basically a rewrite of the original at https://github.com/snowplow/kinesis-example-scala-producer

To run it, setup environment variables with your AWS credentials, e.g.:

export AWS_ACCESS_KEY_ID=[REDACTED]
export AWS_SECRET_ACCESS_KEY=[REDACTED]

Change src/main/resources/default.conf with the stream name you want to publish to, and how often, in ms,
you want to publish.

Then run "sbt run" and it should "just work".

Obviously you need sbt 0.13 or later. The Kinesis client needs JDK 1.7+

Thanks to alexanderdean & SnowPlow for getting me started...

