name := course.value + "-" + assignment.value

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-deprecation")

courseId := "e8VseYIYEeWxQQoymFg8zQ"

resolvers += Resolver.sonatypeRepo("releases")

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % "test"
libraryDependencies ++= assignmentsMap.value.values.flatMap(_.dependencies).toSeq

// include the common dir
commonSourcePackages += "common"

assignmentsMap := {
  val depsSpark = Seq(
    "org.apache.spark" %% "spark-core" % "1.2.1"
  )
  Map(
    "example" -> Assignment(
      packageName = "example",
      key = "9W3VuiJREeaFaw43_UrNUw",
      itemId = "I6L8m",
      partId = "vsJoj",
      maxScore = 10d,
      dependencies = depsSpark,
      options = Map("Xmx"->"1540m", "grader-memory"->"2048"))
  )
}

// javaOptions in Test += "-Xmx4G"
// fork in Test := true

