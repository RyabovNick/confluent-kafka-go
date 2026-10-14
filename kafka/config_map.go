/**
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import (
	"strings"
)

// ConfigValue supports the following types:
//
//	bool, int, string, any type with the standard String() interface
type ConfigValue interface{}

// ConfigMap is a map containing standard librdkafka configuration properties as documented in:
// https://github.com/confluentinc/librdkafka/tree/master/CONFIGURATION.md
//
// The special property "default.topic.config" (optional) is a ConfigMap
// containing default topic configuration properties.
//
// The use of "default.topic.config" is deprecated,
// topic configuration properties shall be specified in the standard ConfigMap.
// For backwards compatibility, "default.topic.config" (if supplied)
// takes precedence.
type ConfigMap map[string]ConfigValue

// SetKey sets configuration property key to value.
//
// For user convenience a key prefixed with {topic}. will be
// set on the "default.topic.config" sub-map, this use is deprecated.
func (m ConfigMap) SetKey(key string, value ConfigValue) error {
	if strings.HasPrefix(key, "{topic}.") {
		_, found := m["default.topic.config"]
		if !found {
			m["default.topic.config"] = ConfigMap{}
		}
		m["default.topic.config"].(ConfigMap)[strings.TrimPrefix(key, "{topic}.")] = value
	} else {
		m[key] = value
	}

	return nil
}

func (m ConfigMap) clone() ConfigMap {
	m2 := make(ConfigMap)
	for k, v := range m {
		m2[k] = v
	}
	return m2
}
