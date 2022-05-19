package net.virtualvoid.restic

import akka.actor.ActorSystem
import com.typesafe.config.Config

import java.io.File

case class ResticSettings(
    repositoryDir:          File,
    userCache:              File,
    localCache:             File,
    repositoryPasswordFile: Option[File]
)

object ResticSettings {
  def apply()(implicit system: ActorSystem): ResticSettings = apply(system.settings.config)
  def apply(config: Config): ResticSettings = {
    val conf = config.getConfig("restic")
    def existingFile(path: String): File = {
      val f = file(path)
      require(f.exists(), s"Did not find the file [$f] specified as [$path]")
      f
    }
    def file(path: String): File =
      new File(conf.getString(path))

    new ResticSettings(
      repositoryDir = existingFile("repository"),
      userCache = file("user-cache-dir"),
      localCache = file("cache-dir"),
      repositoryPasswordFile = if (conf.hasPath("password-file")) Some(existingFile("password-file")) else None
    )
  }
}
