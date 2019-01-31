
package com.dimafeng.testcontainers


import java.net.URI

import com.dimafeng.testcontainers.GenericContainer.DockerImage
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.wait.strategy.{LogMessageWaitStrategy, Wait, WaitStrategy}


class HadoopContainer(dockerImage: DockerImage,
                      exposedPorts: Seq[Int] = Seq(),
                      env: Map[String, String] = Map(),
                      command: Seq[String] = Seq(),
                      classpathResourceMapping: Seq[(String, String, BindMode)] = Seq(),
                      waitStrategy: Option[WaitStrategy] = null)
  extends GenericContainer(dockerImage, exposedPorts, env, command, classpathResourceMapping, waitStrategy) {

  private def hdfsuri = s"hdfs://${containerIpAddress}:${mappedPort(HadoopContainer.DEFAULT_PORT)}"

  private def hadoopConfig: Configuration = new Configuration() {
    hadoopConfig.set("fs.defaultFS", hdfsuri)
  }

  def getFileSystem: FileSystem = FileSystem.get(
    new URI(hdfsuri),
    hadoopConfig)

}

object HadoopContainer {
  val DEFAULT_HADOOP_VERSION = "sequenceiq/hadoop-docker:2.7.0"
  val DEFAULT_PORT = 9000
  val DEFAULT_WAIT_STRATEGY = Wait.forHttp("/dfshealth.jsp")

  def apply(dockerImage: DockerImage = HadoopContainer.DEFAULT_HADOOP_VERSION,
            exposedPorts: Seq[Int] = Seq(DEFAULT_PORT),
            env: Map[String, String] = Map(),
            command: Seq[String] = Seq(),
            classpathResourceMapping: Seq[(String, String, BindMode)] = Seq(),
            waitStrategy: Option[WaitStrategy] = Option(DEFAULT_WAIT_STRATEGY)): HadoopContainer =
    new HadoopContainer(dockerImage, exposedPorts, env, command, classpathResourceMapping, waitStrategy)

}
