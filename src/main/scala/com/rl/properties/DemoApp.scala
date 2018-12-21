package com.rl.properties

/**
  *
  * @author Richard Li
  */
trait DemoApp {
  def setup():Unit = {
    System.setProperty("hadoop.home.dir", "D:\\DevEnv\\winutils");
  }
}
