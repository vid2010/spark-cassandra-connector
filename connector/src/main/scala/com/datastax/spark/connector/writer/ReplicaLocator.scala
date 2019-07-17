package com.datastax.spark.connector.writer


import java.net.InetAddress

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.spark.connector.ColumnSelector
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.Logging
import com.datastax.spark.connector.util.PatitionKeyTools._

import scala.collection.JavaConversions._
import scala.collection._

/**
 * A utility class for determining the Replica Set (Ip Addresses) of a particular Cassandra Row. Used
 * by the [[com.datastax.spark.connector.RDDFunctions.keyByCassandraReplica]] method. Uses the Java
 * Driver to obtain replica information.
 */
class ReplicaLocator[T] private(
    connector: CassandraConnector,
    tableDef: TableDef,
    rowWriter: RowWriter[T]) extends Serializable with Logging {

  val keyspaceName = tableDef.keyspaceName
  val tableName = tableDef.tableName
  val columnNames = rowWriter.columnNames
  val tokenMap = connector.withSessionDo(_.getMetadata.getTokenMap).get //TODO Fix get

  /**
   * Pairs each piece of data with the Cassandra Replicas which that data would be found on
   * @param data A source of data which can be bound to a statement by BatchStatementBuilder
   * @return an Iterator over the same data keyed by the replica's ip addresses
   */
  def keyByReplicas(data: Iterator[T]): Iterator[(scala.collection.immutable.Set[InetAddress], T)] = {
      connector.withSessionDo { session =>
        val protocolVersion = session.getContext.getProtocolVersion
        val stmt = prepareDummyStatement(session, tableDef)
        val routingKeyGenerator = new RoutingKeyGenerator(tableDef, columnNames)
        val boundStmtBuilder = new BoundStatementBuilder(rowWriter, stmt, protocolVersion = protocolVersion)
        val clusterMetadata = session.getMetadata
        data.map { row =>
          val hosts = tokenMap
            .getReplicas(CqlIdentifier.fromInternal(keyspaceName), routingKeyGenerator.apply(boundStmtBuilder.bind(row).stmt))
            .map(_.getBroadcastAddress.get().getAddress)// TODO Fix Get
            .toSet[InetAddress]
          (hosts, row)
        }
    }
  }
}

/**
 * Helper methods for mapping a set of data to their relative locations in a Cassandra Cluster.
 */
object ReplicaLocator {
  def apply[T: RowWriterFactory](
      connector: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      partitionKeyMapper: ColumnSelector): ReplicaLocator[T] = {

    val tableDef = Schema.tableFromCassandra(connector, keyspaceName, tableName)
    val rowWriter = implicitly[RowWriterFactory[T]].rowWriter(
      tableDef,
      partitionKeyMapper.selectFrom(tableDef)
    )
    new ReplicaLocator[T](connector, tableDef, rowWriter)
  }
}
