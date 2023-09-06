import sbt.util

val scalaV = "2.13.11"

val pekkoV = "1.0.1"
val pekkoHttpV = "1.0.0"
val pekkoConnectorsV = "1.0.0"
val sprayJsonV = "1.3.6"

val scalaTestV = "3.2.16"

val aircompressorV = "0.25"

inThisBuild(Def.settings(
  scalaVersion := scalaV,
  //resolvers += "Apache Nexus Snapshots" at "https://repository.apache.org/content/repositories/snapshots/",
  //resolvers += "Apache Nexus Staging" at "https://repository.apache.org/content/repositories/staging/",
  evictionErrorLevel := util.Level.Info,
))

lazy val root = Project(id = "root", base = file(".")).aggregate(core, web)

lazy val core =
  project
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.pekko" %% "pekko-stream" % pekkoV,
        "org.apache.pekko" %% "pekko-http-caching" % pekkoHttpV,
        "org.apache.pekko" %% "pekko-connectors-file" % pekkoConnectorsV,
        "io.spray" %% "spray-json" % sprayJsonV,
        "io.airlift" % "aircompressor" % aircompressorV,
        "com.lambdaworks" % "scrypt" % "1.4.0",
        "org.scalatest" %% "scalatest" % scalaTestV % "test",
      )
    )
    .enablePlugins(ParadoxMaterialThemePlugin)
    .settings(
      Compile / paradoxMaterialTheme := {
        ParadoxMaterialTheme()
          // choose from https://jonas.github.io/paradox-material-theme/getting-started.html#changing-the-color-palette
          .withColor("light-green", "amber")
          // choose from https://jonas.github.io/paradox-material-theme/getting-started.html#adding-a-logo
          .withLogoIcon("cloud")
          .withCopyright("Copyleft © Johannes Rudolph")
          .withRepository(uri("https://github.com/jrudolph/xyz"))
          .withSocial(
            uri("https://github.com/jrudolph"),
            uri("https://twitter.com/virtualvoid")
          )
      },
      paradoxProperties ++= Map(
        "github.base_url" -> (Compile / paradoxMaterialTheme).value.properties.getOrElse("repo", "")
      )
    )

lazy val web =
  project
    .dependsOn(core)
    .enablePlugins(SbtTwirl, BuildInfoPlugin)
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.pekko" %% "pekko-http" % pekkoHttpV,
        "org.scalatest" %% "scalatest" % scalaTestV % "test",
      ),
      // Fix broken watchSources support in play/twirl, https://github.com/playframework/twirl/issues/186
      // watch sources support
      watchSources +=
        WatchSource(
          (TwirlKeys.compileTemplates / sourceDirectory).value,
          "*.scala.*",
          (excludeFilter in Global).value
        ),

      buildInfoPackage := "net.virtualvoid.restic.web",
      buildInfoKeys ++= Seq(
        "longProjectName" -> "Restic Repository Browser"
      ),
    )