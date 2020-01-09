/*
 * Copyright 2020 Kai Zimmermann
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package kaizimmerm.samples.azure.eventhubs;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class KafkaClientConfigProperties {

  /**
   * @see CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG
   */
  private String bootstrapServers;

  /**
   * @see CommonClientConfigs#CLIENT_ID_CONFIG
   */
  private String clientIdPrefix = "SampleClient";

  /**
   * @see CommonClientConfigs#SECURITY_PROTOCOL_CONFIG
   */
  private SecurityProtocol securityProtocol = SecurityProtocol.SASL_SSL;

  /**
   * @see SaslConfigs#SASL_MECHANISM
   */
  private String saslMechanism = "PLAIN";

  /**
   * @see SaslConfigs#SASL_JAAS_CONFIG
   */
  private String saslJaasConfig;

  /**
   * @see ProducerConfig#ACKS_CONFIG
   */
  private String acks;

  /**
   * @see ProducerConfig#KEY_SERIALIZER_CLASS_CONFIG
   */
  private Class<?> keySerializer = StringSerializer.class;

  /**
   * @see ProducerConfig#VALUE_SERIALIZER_CLASS_CONFIG
   */
  private Class<?> valueSerializer = StringSerializer.class;

  /**
   * @see ConsumerConfig#KEY_DESERIALIZER_CLASS_CONFIG
   */
  private Class<?> keyDeserializer = StringDeserializer.class;

  /**
   * @see ConsumerConfig#VALUE_DESERIALIZER_CLASS_CONFIG
   */
  private Class<?> valueDeserializer = StringDeserializer.class;

  /**
   * @see CommonClientConfigs#RETRIES_CONFIG
   */
  private Integer retries;

  /**
   * @see ConsumerConfig#MAX_POLL_INTERVAL_MS_CONFIG
   */
  private Duration maxPoll = Duration.ofMinutes(5);

  /**
   * @see ConsumerConfig#GROUP_ID_CONFIG
   */
  private String groupId = "$Default";

  /**
   * @see ConsumerConfig#AUTO_OFFSET_RESET_CONFIG
   */
  private String autoOffsetReset = "earliest";

  /**
   * @see ConsumerConfig#ENABLE_AUTO_COMMIT_CONFIG
   */
  private Boolean enableAutoCommit = false;

  /**
   * @see CommonClientConfigs#REQUEST_TIMEOUT_MS_CONFIG
   */
  private Long requestTimeoutMs = Long.valueOf(60_000);


  public Map<String, String> buildProducerProperties(final String clientId) {
    final Map<String, String> result = buildCommonProperties(clientId);

    if (!Objects.isNull(acks)) {
      result.put(ProducerConfig.ACKS_CONFIG, acks);
    }

    if (!Objects.isNull(keySerializer)) {
      result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getCanonicalName());
    }

    if (!Objects.isNull(valueSerializer)) {
      result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getCanonicalName());
    }

    return result;
  }

  public Map<String, String> buildConsumerProperties(final String clientId) {
    final Map<String, String> result = buildCommonProperties(clientId);


    if (!Objects.isNull(groupId)) {
      result.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    if (!Objects.isNull(autoOffsetReset)) {
      result.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
    }

    if (!Objects.isNull(enableAutoCommit)) {
      result.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString());
    }

    if (!Objects.isNull(keyDeserializer)) {
      result.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getCanonicalName());
    }

    if (!Objects.isNull(valueDeserializer)) {
      result.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          valueDeserializer.getCanonicalName());
    }

    if (!Objects.isNull(maxPoll)) {
      result.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(maxPoll.toMillis()));
    }
    return result;
  }


  private Map<String, String> buildCommonProperties(final String clientId) {

    final Map<String, String> result = new HashMap<>();

    if (!Objects.isNull(bootstrapServers)) {
      result.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    if (Objects.isNull(clientIdPrefix)) {
      result.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
    } else {
      result.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientIdPrefix + clientId);
    }

    if (!Objects.isNull(securityProtocol)) {
      result.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.toString());
    }

    if (!Objects.isNull(saslMechanism)) {
      result.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
    }

    if (!Objects.isNull(saslJaasConfig)) {
      result.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
    }

    if (!Objects.isNull(retries)) {
      result.put(CommonClientConfigs.RETRIES_CONFIG, retries.toString());
    }

    if (!Objects.isNull(requestTimeoutMs)) {
      result.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs.toString());
    }


    return result;
  }

}
