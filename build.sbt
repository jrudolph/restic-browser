
val scalaV = "2.13.8"

val akkaV = "2.6.19"
val akkaHttpV = "10.2.9"
val sprayJsonV = "1.3.6"

val scalaTestV = "3.2.11"

inThisBuild(Def.settings(
  scalaVersion := scalaV
))

lazy val root = Project(id = "root", base = file(".")).aggregate(core, web)

lazy val core =
  project
    .settings(
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream" % akkaV,
        "com.typesafe.akka" %% "akka-http-caching" % akkaHttpV,
        "io.spray" %% "spray-json" % sprayJsonV,
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
        "com.typesafe.akka" %% "akka-http" % akkaHttpV,
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