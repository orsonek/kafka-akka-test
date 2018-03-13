package example

import java.io.File
import java.nio.file.Files
import java.util.Properties

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.scalatest._

import scala.concurrent.Future

class KafkaSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
    val ZOOKEEPER_HOST = "localhost"
    val ZOOKEEPER_PORT = 21810
    val KAFKA_BROKER_HOST = "localhost"
    val KAFKA_BROKER_PORT = 19091


    implicit var system: ActorSystem = _
    implicit var materializer: ActorMaterializer = _

    implicit var kafkaLogDir: File = _

    implicit var zooKeeper: TestingServer = _
    implicit var kafka: KafkaServerStartable = _
    implicit var curator: CuratorFramework = _

    import scala.concurrent.ExecutionContext.Implicits.global

    // ---------------------------------------------------------------------------------------------------------------------
    // LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE
    // ---------------------------------------------------------------------------------------------------------------------

    /**
      * Test lifecycle - before
      */
    override def beforeAll( ): Unit = {

        // start actor system
        system = ActorSystem( "akka-zoo-kafka" )
        materializer = ActorMaterializer()( system )

        // log directory removed after exit
        kafkaLogDir = Files.createTempDirectory( "kafka-tmp-dev-logs" ).toFile
        kafkaLogDir.deleteOnExit()

        // zoo..
        zooKeeper = new TestingServer( ZOOKEEPER_PORT )
        zooKeeper.start()

        // ..and kafka start
        val kafkaProperties = new Properties()
        kafkaProperties.setProperty( "zookeeper.connect", s"$ZOOKEEPER_HOST:$ZOOKEEPER_PORT" )
        kafkaProperties.setProperty( "broker.id", "1" )
        kafkaProperties.setProperty( "host.name", s"$KAFKA_BROKER_HOST" )
        kafkaProperties.setProperty( "port", Integer.toString( KAFKA_BROKER_PORT ) )
        kafkaProperties.setProperty( "log.dir", kafkaLogDir.getAbsolutePath )
        kafkaProperties.setProperty( "log.flush.interval.messages", String.valueOf( 1 ) )
        kafkaProperties.setProperty( "delete.topic.enable", String.valueOf( true ) )
        kafkaProperties.setProperty( "offsets.topic.replication.factor", String.valueOf( 1 ) )
        kafkaProperties.setProperty( "auto.create.topics.enable", String.valueOf( true ) )

        kafka = new KafkaServerStartable( new KafkaConfig( kafkaProperties ) )
        kafka.startup()

        // curator start
        curator = CuratorFrameworkFactory.builder()
            .retryPolicy( new ExponentialBackoffRetry( 1000, 3 ) )
            .namespace( "tests" )
            .connectString( s"$ZOOKEEPER_HOST:$ZOOKEEPER_PORT" ).build()
        curator.start()
    }

    /**
      * Test lifecycle - after
      */
    override def afterAll( ): Unit = {
        Thread.sleep( 20000 )
        //system
        kafka.shutdown()
        curator.close()
        zooKeeper.stop()
    }



    // ---------------------------------------------------------------------------------------------------------------------
    // TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS
    // ---------------------------------------------------------------------------------------------------------------------

    // ZOO Keeper test

    it must "create zk node" in {
        curator.create().orSetData().forPath( "/sparta", "THIS IS SPARTA".getBytes() )
    }

    it must "read created zk node" in {
        val d: Array[ Byte ] = curator.getData.forPath( "/sparta" )
        val v = new String( d )
        assert( v.equals( "THIS IS SPARTA" ) )
    }

    it must "produce some messages to topic" in {
        val producerSettings = ProducerSettings( system, new ByteArraySerializer, new StringSerializer )
            .withBootstrapServers( s"$KAFKA_BROKER_HOST:$KAFKA_BROKER_PORT" )

        Source( 1 to 100 )
            .map( i => s"King's Announcement #$i" )
            .map { elem =>
                new ProducerRecord[ Array[ Byte ], String ]( "sparta", elem )
            }
            .runWith( Producer.plainSink( producerSettings ) )
    }

    it must "read some messages from topic (commitable in Kafka)" in {

        val consumerSettings = ConsumerSettings( system, new ByteArrayDeserializer, new StringDeserializer )
            .withBootstrapServers( s"$KAFKA_BROKER_HOST:$KAFKA_BROKER_PORT" )
            .withGroupId( "people-of-sparta" )
            .withProperty( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" )

        // source may be committableSource, plainSource or atMostOnceSource
        Consumer.committableSource( consumerSettings, Subscriptions.topics( "sparta" ) )
            .mapAsync( 1 ) { msg =>
                println( msg.record.value )
                Future( msg )
            }
            .mapAsync( 1 ) { msg =>
                msg.committableOffset.commitScaladsl()
            }
            .runWith( Sink.ignore )
    }

    // TODO Kafka test


}


