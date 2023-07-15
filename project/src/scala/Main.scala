package src.scala

import src.scala.config.{configBuilder, factoryProvider}

object Main {
  def main(args: Array[String]): Unit = {
    val config = new configBuilder().getConfigurationBuilder
    val factory = new factoryProvider(config)
    val twitterFactory = factory.getTwitterFactory
    val twitter = twitterFactory.getInstance()

    // Utilisez l'instance `twitter` pour interagir avec l'API Twitter
  }
}
