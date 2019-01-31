package com.dimafeng.testcontainers.integration

import java.sql.DriverManager

import com.dimafeng.testcontainers.{ForAllTestContainer, HadoopContainer, MySQLContainer}
import org.apache.hadoop.fs.Path
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy

class HadoopSpec extends FlatSpec with ForAllTestContainer {

  override val container = HadoopContainer(
    command = Seq("/etc/bootstrap.sh", "-d"),
    exposedPorts = Seq(50070, 50010, 50020, 50075, 50090, 8020, 9000),
    waitStrategy = Option(new LogMessageWaitStrategy().withRegEx(".*localhost: starting nodemanager.*\n"
    )))

  "Hadoop container" should "be started" in {
    print("started")
    container.getFileSystem.create(new Path("./1.txt")).writeUTF("Hello World!")
    assert(container.getFileSystem.exists(new Path("./1.txt")))
  }
}
