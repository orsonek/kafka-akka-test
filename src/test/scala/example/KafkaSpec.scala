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
    val ZOOKEEPER_PORT = 21810
    val KAFKA_BROKER_PORT = 19091
    var testingServer = new TestingServer( ZOOKEEPER_PORT )
    implicit var curator: CuratorFramework = _
    implicit var broker: KafkaServerStartable = _

    val logDir: File = Files.createTempDirectory( "kafka" ).toFile

    logDir.deleteOnExit()

    val brokerProperties = new Properties()
    brokerProperties.setProperty( "zookeeper.connect", "127.0.0.1:21810" )
    brokerProperties.setProperty( "broker.id", "1" )
    brokerProperties.setProperty( "host.name", "localhost" )
    brokerProperties.setProperty( "port", Integer.toString( KAFKA_BROKER_PORT ) )
    brokerProperties.setProperty( "log.dir", logDir.getAbsolutePath )
    brokerProperties.setProperty( "log.flush.interval.messages", String.valueOf( 1 ) )
    brokerProperties.setProperty( "delete.topic.enable", String.valueOf( true ) )
    brokerProperties.setProperty( "offsets.topic.replication.factor", String.valueOf( 1 ) )
    brokerProperties.setProperty( "auto.create.topics.enable", String.valueOf( false ) )


    override def beforeAll( ): Unit = {
        testingServer.start()
        curator = CuratorFrameworkFactory.builder()
            .retryPolicy( new ExponentialBackoffRetry( 1000, 3 ) )
            .namespace( "tests" )
            .connectString( s"127.0.0.1:$ZOOKEEPER_PORT" ).build()
        curator.start()


        broker = new KafkaServerStartable( new KafkaConfig( brokerProperties ) )
        broker.startup()

    }

    it must "create new node" in {
        curator.create().orSetData().forPath( "/iqtech", "THIS IS SPARTA".getBytes() )
        val d: Array[ Byte ] = curator.getData.forPath( "/iqtech" )
        val v = new String( d )
        println( v )
        assert( v.equals( "THIS IS SPARTA" ) )
    }

    override def afterAll( ): Unit = {
        broker.shutdown()
        curator.close()
        testingServer.stop()
    }

}


