package com.mesosphere.dcos.cassandra.common.config;

import com.mesosphere.sdk.config.Configuration;

import java.util.List;

public interface ConfigValidation {
    List<ConfigValidationError> validate(Configuration oldConfig, Configuration newConfig);
}
