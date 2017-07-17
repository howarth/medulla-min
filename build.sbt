name := "medulla"
version := "0.1.0"
scalaVersion := "2.11.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"
resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
libraryDependencies +=  "com.typesafe.play" %% "play" % "2.5.12"

libraryDependencies  ++= Seq(
      "org.scalanlp" %% "breeze" % "0.13.1",
      "org.scalanlp" %% "breeze-natives" % "0.13.1",
      "org.scalanlp" %% "breeze-viz" % "0.13.1"
    )


resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

fork in console := true
fork in run := true
javaOptions += "-Xmx4G"
scalacOptions += "-target:jvm-1.7"

