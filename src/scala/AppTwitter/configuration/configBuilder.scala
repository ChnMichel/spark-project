import twitter4j.conf.ConfigurationBuilder

class configBuilder {
  def getConfigurationBuilder: ConfigurationBuilder = {
    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey("")
      .setOAuthConsumerSecret("")
      .setOAuthAccessToken("")
      .setOAuthAccessTokenSecret("")
    cb
  }
}