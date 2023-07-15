import twitter4j.TwitterFactory
import twitter4j.conf.ConfigurationBuilder

class factoryProvider(config: ConfigurationBuilder) {
  def getTwitterFactory: TwitterFactory = {
    val tf = new TwitterFactory(config.build())
    tf
  }
}
