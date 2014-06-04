import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

assemblySettings

name := "gxdgroup"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "0.9.0-incubating",
  "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.3.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "0.9.0-incubating" % "provided",
  "net.debasishg" % "redisclient_2.10" % "2.12",
  "redis.clients" % "jedis" % "2.1.0",
  "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test"
)

resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

resolvers += Resolver.url("cloudera", url("https://repository.cloudera.com/artifactory/cloudera-repos/."))

resolvers += Resolver.url("MavenOfficial", url("http://repo1.maven.org/maven2"))

resolvers += Resolver.url("springside", url("http://springside.googlecode.com/svn/repository"))

resolvers += Resolver.url("jboss", url("http://repository.jboss.org/nexus/content/groups/public-jboss"))

//resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

//resolvers += "Local Maven Repository" at "file:///Users/wq/lib/m2/repository"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("com", "esotericsoftware", "minlog", xs @ _*) => MergeStrategy.first
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*)=> MergeStrategy.first
  case PathList("org", "eclipse", xs @ _*) => MergeStrategy.first
  case PathList("akka",  xs @ _*)=> MergeStrategy.first
  case "application.conf" => MergeStrategy.concat
  case "unwanted.txt"     => MergeStrategy.discard
  case x => old(x)
}
}