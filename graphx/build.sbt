// Alternative a spark-shell : compiler un jar et le lancer avec spark-submit
//   sbt package
//   spark-submit --class SahlPayGraphX --master spark://spark-master:7077 \
//                target/scala-2.12/sahlpay-graphx_2.12-1.0.jar
name := "sahlpay-graphx"
version := "1.0"
scalaVersion := "2.12.18"          // meme version de Scala que Spark 3.5.3

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"   % "3.5.3" % "provided",
  "org.apache.spark" %% "spark-sql"    % "3.5.3" % "provided",
  "org.apache.spark" %% "spark-graphx" % "3.5.3" % "provided"
)
