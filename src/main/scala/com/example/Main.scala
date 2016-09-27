package com.example

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.RegionUtils
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, RangeKeyCondition, Table, Item}

import scala.collection.JavaConverters._

class DynamoDBClient(tableName: String, hashKeyName: String, rangeKeyName: String) {

  lazy val underlying: AmazonDynamoDBClient = {
    val accessKey = sys.env("AWS_ACCESS_KEY_ID")
    val secretKey = sys.env("AWS_SECRET_ACCESS_KEY")
    val region = RegionUtils.getRegion("ap-northeast-1")
    val endpoint = region.getServiceEndpoint("dynamodb")
    val creds = new BasicAWSCredentials(accessKey, secretKey)
    val conf = new ClientConfiguration()
      .withRetryPolicy(PredefinedRetryPolicies.NO_RETRY_POLICY)

    val client = new AmazonDynamoDBClient(creds, conf)
    client.setRegion(region)
    client.setEndpoint(endpoint)
    client
  }

  lazy val dynamoDB = new DynamoDB(underlying)
  lazy val table: Table = dynamoDB.getTable(tableName)

  def putItem(item: Item): Unit = table.putItem(item)

  def query(
    hashKey: AnyRef,
    rangeKeyFrom: AnyRef,
    rangeKeyTo: AnyRef,
    limit: Int,
    orderByAsc: Boolean = true): Seq[Item] = {

    val spec = new QuerySpec()
      .withHashKey(hashKeyName, hashKey)
      .withRangeKeyCondition(
        new RangeKeyCondition(rangeKeyName)
          .between(rangeKeyFrom, rangeKeyTo)
      )
      .withMaxResultSize(limit)
      .withScanIndexForward(orderByAsc)

    table.query(spec).asScala.toSeq
  }
}

object Main {
  val loremIpsum = """Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean in orci vel quam placerat porttitor. Integer a urna sed erat tempor tincidunt non ut justo. Donec non dapibus est. Suspendisse volutpat odio et tellus finibus pellentesque. Suspendisse elementum magna urna, quis dictum neque hendrerit ut. In nec maximus velit. In molestie interdum elit eu tincidunt. Vestibulum ullamcorper diam eget faucibus scelerisque. Praesent dictum, mi id dapibus rhoncus, odio nunc porttitor ante, vitae varius dolor quam ut enim.
                     |
                     |Donec a sem consectetur, tempus neque a, rhoncus lectus. Pellentesque quis gravida ipsum. Curabitur tincidunt, velit a laoreet commodo, nisl mi feugiat eros, quis mollis justo nibh at sem. Duis tincidunt libero eget risus fringilla, sed rhoncus elit rutrum. Cras quis tortor sapien. Suspendisse semper ullamcorper iaculis. Maecenas non fermentum erat, sit amet porttitor purus. Quisque faucibus pretium erat nec egestas. Praesent ac lacus facilisis, dignissim mi nec, ultrices odio. Curabitur tellus metus.""".stripMargin

  val items = (1L to 1000L).map { t =>
    new Item()
      .withString("cookieId", "foo")
      .withLong("timestamp", t)
      .withString("content", loremIpsum)
  }

  def main(args: Array[String]): Unit = {

    val client = new DynamoDBClient(
      "com.mayreh.example",
      "cookieId",
      "timestamp"
    )

    args.toList match {
      case "put" :: _ =>
        println("put example items.")

        items.foreach(client.putItem)

      case "query" :: limit :: times :: _ =>
        println("query items.")

        (1 to times.toInt).foreach { _ =>
          val fetchedItems =
            client.query("foo", long2Long(1L), long2Long(1000L), limit.toInt, orderByAsc = false)

          println(s"************** returned ${fetchedItems.size} items. **************")
        }
      case _ =>
        println("wrong args.")
    }
  }
}
