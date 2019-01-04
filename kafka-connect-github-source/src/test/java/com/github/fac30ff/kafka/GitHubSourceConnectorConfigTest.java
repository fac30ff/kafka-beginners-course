package com.github.fac30ff.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

public class GitHubSourceConnectorConfigTest {

    private ConfigDef configDef = GitHubSourceConnectorConfig.conf();

    private Map<String, String> initialConfig() {
        Map<String, String> baseProps = new HashMap<>();
        baseProps.put(GitHubSourceConnectorConfig.OWNER_CONFIG, "foo");
        baseProps.put(GitHubSourceConnectorConfig.REPO_CONFIG, "bar");
        baseProps.put(GitHubSourceConnectorConfig.SINCE_CONFIG, "2017-04-26T01:23:45Z");
        baseProps.put(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG, "100");
        baseProps.put(GitHubSourceConnectorConfig.TOPIC_CONFIG, "github-issues");
        return baseProps;
    }


    @Test
    public void doc() {
        System.out.println(GitHubSourceConnectorConfig.conf().toRst());
    }

    @Test
    public void initialConfigIsValid() {
        assert (configDef.validate(initialConfig())
                .stream()
                .allMatch(configValue -> configValue.errorMessages().size() == 0));
    }

    @Test
    public void canReadConfigCorrectly() {
        GitHubSourceConnectorConfig config = new GitHubSourceConnectorConfig(initialConfig());
        config.getAuthPassword();

    }


    @Test
    public void validateSince() {
        Map<String, String> config = initialConfig();
        config.put(GitHubSourceConnectorConfig.SINCE_CONFIG, "not-a-date");
        ConfigValue configValue = configDef.validateAll(config).get(GitHubSourceConnectorConfig.SINCE_CONFIG);
        assert (configValue.errorMessages().size() > 0);
    }

    @Test
    public void validateBatchSize() {
        Map<String, String> config = initialConfig();
        config.put(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG, "-1");
        ConfigValue configValue = configDef.validateAll(config).get(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG);
        assert (configValue.errorMessages().size() > 0);

        config = initialConfig();
        config.put(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG, "101");
        configValue = configDef.validateAll(config).get(GitHubSourceConnectorConfig.BATCH_SIZE_CONFIG);
        assert (configValue.errorMessages().size() > 0);

    }

    @Test
    public void validateUsername() {
        Map<String, String> config = initialConfig();
        config.put(GitHubSourceConnectorConfig.AUTH_USERNAME_CONFIG, "username");
        ConfigValue configValue = configDef.validateAll(config).get(GitHubSourceConnectorConfig.AUTH_USERNAME_CONFIG);
        assert (configValue.errorMessages().size() == 0);
    }

    @Test
    public void validatePassword() {
        Map<String, String> config = initialConfig();
        config.put(GitHubSourceConnectorConfig.AUTH_PASSWORD_CONFIG, "password");
        ConfigValue configValue = configDef.validateAll(config).get(GitHubSourceConnectorConfig.AUTH_PASSWORD_CONFIG);
        assert (configValue.errorMessages().size() == 0);
    }

}