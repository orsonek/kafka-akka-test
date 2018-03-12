package example

import java.io.File
import java.nio.file.Files
import java.util.Properties

import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.test.TestingServer
import org.apache.curator.retry.ExponentialBackoffRetry
import org.scalatest._

class KafkaSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
    val ZOOKEEPER_HOST = "localhost"
    val ZOOKEEPER_PORT = 21810
    val KAFKA_BROKER_HOST = "localhost"
    val KAFKA_BROKER_PORT = 19091

    implicit var logDir: File = _

    implicit var zooKeeper: TestingServer = _
    implicit var kafka: KafkaServerStartable = _
    implicit var curator: CuratorFramework = _

    // ---------------------------------------------------------------------------------------------------------------------
    // LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE   LIFECYCLE
    // ---------------------------------------------------------------------------------------------------------------------

    /**
      * Test lifecycle - before
      */
    override def beforeAll( ): Unit = {
        // log directory removed after exit
        logDir = Files.createTempDirectory( "kafka" ).toFile
        logDir.deleteOnExit()

        // zoo..
        zooKeeper = new TestingServer( ZOOKEEPER_PORT )
        zooKeeper.start()

        // ..and kafka start
        val kafkaProperties = new Properties()
        kafkaProperties.setProperty( "zookeeper.connect", s"$ZOOKEEPER_HOST:$ZOOKEEPER_PORT" )
        kafkaProperties.setProperty( "broker.id", "1" )
        kafkaProperties.setProperty( "host.name", s"$KAFKA_BROKER_HOST" )
        kafkaProperties.setProperty( "port", Integer.toString( KAFKA_BROKER_PORT ) )
        kafkaProperties.setProperty( "log.dir", logDir.getAbsolutePath )
        kafkaProperties.setProperty( "log.flush.interval.messages", String.valueOf( 1 ) )
        kafkaProperties.setProperty( "delete.topic.enable", String.valueOf( true ) )
        kafkaProperties.setProperty( "offsets.topic.replication.factor", String.valueOf( 1 ) )
        kafkaProperties.setProperty( "auto.create.topics.enable", String.valueOf( false ) )

        kafka = new KafkaServerStartable( new KafkaConfig( kafkaProperties ) )
        kafka.startup()

        // curator start
        curator = CuratorFrameworkFactory.builder()
            .retryPolicy( new ExponentialBackoffRetry( 1000, 3 ) )
            .namespace( "tests" )
            .connectString( s"127.0.0.1:$ZOOKEEPER_PORT" ).build()
        curator.start()
    }

    /**
      * Test lifecycle - after
      */
    override def afterAll( ): Unit = {
        kafka.shutdown()
        curator.close()
        zooKeeper.stop()
    }


    // ---------------------------------------------------------------------------------------------------------------------
    // TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS   TESTS
    // ---------------------------------------------------------------------------------------------------------------------

    // ZOO Keeper test

    it must "create new node" in {
        curator.create().orSetData().forPath( "/iqtech", "THIS IS SPARTA".getBytes() )
    }

    it must "read created node" in {
        val d: Array[ Byte ] = curator.getData.forPath( "/iqtech" )
        val v = new String( d )
        println( v )
        assert( v.equals( "THIS IS SPARTA" ) )
    }

    // TODO Kafka test


}


