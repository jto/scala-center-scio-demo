addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.8.0")
addSbtPlugin("com.cavorite" % "sbt-avro" % "3.2.0")

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro-compiler" % "1.8.2"
)
