package com.github.fac30ff.kafka.tutorial3;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.ConfigurationBuilder;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.builder.fluent.PropertiesBuilderParameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;
import java.util.Optional;

public class PropertiesLoader {
    private static volatile PropertiesLoader propertiesLoader = null;
    private static final String creds = "creds.properties";
    private Configuration credProperties;

    private PropertiesLoader() throws ConfigurationException {
        credProperties = getCredProperties();
    }

    public static PropertiesLoader getPropertiesLoader() throws ConfigurationException {
        if (propertiesLoader == null) {
            synchronized (PropertiesLoader.class) {
                if (propertiesLoader == null) {
                    propertiesLoader = new PropertiesLoader();
                }
            }
        }
        return propertiesLoader;
    }

    private Configuration getCredProperties() throws ConfigurationException {
        PropertiesBuilderParameters parameters = new Parameters()
                .properties()
                .setFile(new File(creds))
                .setThrowExceptionOnMissing(true)
                .setListDelimiterHandler(new DefaultListDelimiterHandler('='));
        ConfigurationBuilder<PropertiesConfiguration> configurationBuilder =
                new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                .configure(parameters);
        return configurationBuilder.getConfiguration();
    }

    public String asString(String parameter) throws NoCredentialIsAvailable {
        return Optional.ofNullable(this.credProperties.getString(parameter)).orElseThrow(NoCredentialIsAvailable::new);
    }

    private class NoCredentialIsAvailable extends Exception {

        NoCredentialIsAvailable(){
            errorMessage();
        }

        NoCredentialIsAvailable(String message) {
            super(message);
        }

        public String getMessage() {
            return super.getMessage();
        }

        private String errorMessage() {
            return "no credential is available";
        }
    }
}
