package com.github.fac30ff.kafka;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class GitHubSourceConnectorTest {

  private Map<String, String> initialConfig() {
    Map<String, String> baseProps = new HashMap<>();
    baseProps.put(GitHubSourceConnectorConfig.OWNER_CONFIG, "foo");
    baseProps.put(GitHubSourceConnectorConfig.REPO_CONFIG, "bar");
    baseProps.put(GitHubSourceConnectorConfig.SINCE_CONFIG, "2017-04-26T01:23:45Z");
    baseProps.put(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG, "100");
    baseProps.put(GitHubSourceConnectorConfig.TOPIC_CONFIG, "github-issues");
    return (baseProps);
  }

  @Test
  public void taskConfigsShouldReturnOneTaskConfig() {
      GitHubSourceConnector gitHubSourceConnector = new GitHubSourceConnector();
      gitHubSourceConnector.start(initialConfig());
      assertEquals(gitHubSourceConnector.taskConfigs(1).size(),1);
      assertEquals(gitHubSourceConnector.taskConfigs(10).size(),1);
  }
}
