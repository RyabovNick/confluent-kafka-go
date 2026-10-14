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

// Serializer turns a typed key or value into the bytes produced to Kafka.
//
// Warning: THIS IS AN EXPERIMENTAL API, SUBJECT TO CHANGE OR REMOVAL.
type Serializer interface {
	SerializeWithHeaders(topic string, msg interface{}) ([]Header, []byte, error)

	// SetClusterIDResolver hands the serializer a resolver for the ID of the
	// Kafka cluster the [SerializingProducer] is connected to, for the part of
	// its configuration that depends on it.
	//
	// The resolver is invoked whenever the serializer actually needs the ID,
	// never while the producer is being built, so construction never waits on
	// a broker - which it could not reach anyway when, for instance, the
	// OAUTHBEARER token refresh callback is only served once the producer is
	// polling. It may block for up to a minute while the producer reaches a
	// broker, and reports an error if it cannot.
	//
	// The resolver is bound to the producer that supplied it and fails once
	// that producer is closed, so a serializer must not outlive the producer
	// it was given to, unless the cluster ID it needs was configured
	// explicitly. Implementations that do not use the cluster ID, or that had
	// one configured explicitly, ignore the resolver.
	SetClusterIDResolver(resolve func() (string, error))

	// Close releases the resources the serializer created itself. Resources
	// the application supplied are left alone.
	Close() error
}

// SerializerBuilder creates the [Serializer] a [SerializingProducer] uses for
// its keys or its values.
//
// Build is given the producer's [ConfigMap] and whether it is building the key
// serializer, and returns the serializer together with the ConfigMap to carry
// on with: any property the serializer consumed itself is filtered out, so that
// what reaches [NewProducer] holds Kafka properties only.
//
// Warning: THIS IS AN EXPERIMENTAL API, SUBJECT TO CHANGE OR REMOVAL.
type SerializerBuilder interface {
	Build(conf *ConfigMap, isKey bool) (Serializer, *ConfigMap, error)
}

// Deserializer turns the bytes consumed from Kafka into a typed key or value.
//
// Warning: THIS IS AN EXPERIMENTAL API, SUBJECT TO CHANGE OR REMOVAL.
type Deserializer interface {
	DeserializeWithHeaders(topic string, headers []Header, payload []byte) (interface{}, error)

	// SetClusterIDResolver hands the deserializer a resolver for the ID of the
	// Kafka cluster the [DeserializingConsumer] is connected to, for the part
	// of its configuration that depends on it.
	//
	// The resolver is invoked whenever the deserializer actually needs the ID,
	// never while the consumer is being built, so construction never waits on
	// a broker - which it could not reach anyway when, for instance, the
	// OAUTHBEARER token refresh callback is only served from Poll. By the time
	// a message has been fetched the metadata is already cached, so the
	// resolver returns at once. It may block for up to a minute while the
	// consumer reaches a broker, and reports an error if it cannot.
	//
	// The resolver is bound to the consumer that supplied it and fails once
	// that consumer is closed, so a deserializer must not outlive the consumer
	// it was given to, unless the cluster ID it needs was configured
	// explicitly. Implementations that do not use the cluster ID, or that had
	// one configured explicitly, ignore the resolver.
	SetClusterIDResolver(resolve func() (string, error))

	// Close releases the resources the deserializer created itself. Resources
	// the application supplied are left alone.
	Close() error
}

// DeserializerBuilder creates the [Deserializer] a [DeserializingConsumer] uses
// for its keys or its values.
//
// Build is given the consumer's [ConfigMap] and whether it is building the key
// deserializer, and returns the deserializer together with the ConfigMap to
// carry on with: any property the deserializer consumed itself is filtered out,
// so that what reaches [NewConsumer] holds Kafka properties only.
//
// Warning: THIS IS AN EXPERIMENTAL API, SUBJECT TO CHANGE OR REMOVAL.
type DeserializerBuilder interface {
	Build(conf *ConfigMap, isKey bool) (Deserializer, *ConfigMap, error)
}
