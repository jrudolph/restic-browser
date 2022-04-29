val scalaV = "2.13.8"
val scalaTestV = "3.2.10"

inThisBuild(Def.settings(
  scalaVersion := scalaV
))

lazy val root = Project(id = "root", base = file(".")).aggregate(core)

lazy val core =
  project
    .settings(
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-stream" % "2.6.19",
        "io.spray" %% "spray-json" % "1.3.6",
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