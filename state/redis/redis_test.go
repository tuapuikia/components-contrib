// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	redis "github.com/go-redis/redis/v7"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

func TestGetKeyVersion(t *testing.T) {
	store := NewRedisStateStore(logger.NewLogger("test"))
	t.Run("With all required fields", func(t *testing.T) {
		key, ver, err := store.getKeyVersion([]interface{}{"data", "TEST_KEY", "version", "TEST_VER"})
		assert.Equal(t, nil, err, "failed to read all fields")
		assert.Equal(t, "TEST_KEY", key, "failed to read key")
		assert.Equal(t, "TEST_VER", ver, "failed to read version")
	})
	t.Run("With missing data", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{"version", "TEST_VER"})
		assert.NotNil(t, err, "failed to respond to missing data field")
	})
	t.Run("With missing version", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{"data", "TEST_KEY"})
		assert.NotNil(t, err, "failed to respond to missing version field")
	})
	t.Run("With all required fields - out of order", func(t *testing.T) {
		key, ver, err := store.getKeyVersion([]interface{}{"version", "TEST_VER", "dragon", "TEST_DRAGON", "data", "TEST_KEY"})
		assert.Equal(t, nil, err, "failed to read all fields")
		assert.Equal(t, "TEST_KEY", key, "failed to read key")
		assert.Equal(t, "TEST_VER", ver, "failed to read version")
	})
	t.Run("With no fields", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{})
		assert.NotNil(t, err, "failed to respond to missing fields")
	})
	t.Run("With wrong fields", func(t *testing.T) {
		_, _, err := store.getKeyVersion([]interface{}{"dragon", "TEST_DRAGON"})
		assert.NotNil(t, err, "failed to respond to missing fields")
	})
}

func TestParseEtag(t *testing.T) {
	store := NewRedisStateStore(logger.NewLogger("test"))
	t.Run("Empty ETag", func(t *testing.T) {
		ver, err := store.parseETag(&state.SetRequest{
			ETag: "",
		})
		assert.Equal(t, nil, err, "failed to parse ETag")
		assert.Equal(t, 0, ver, "default version should be 0")
	})
	t.Run("Number ETag", func(t *testing.T) {
		ver, err := store.parseETag(&state.SetRequest{
			ETag: "354",
		})
		assert.Equal(t, nil, err, "failed to parse ETag")
		assert.Equal(t, 354, ver, "version should be 254")
	})
	t.Run("String ETag", func(t *testing.T) {
		_, err := store.parseETag(&state.SetRequest{
			ETag: "dragon",
		})
		assert.NotNil(t, err, "shouldn't recognize string ETag")
	})
	t.Run("Concurrency=LastWrite", func(t *testing.T) {
		ver, err := store.parseETag(&state.SetRequest{
			Options: state.SetStateOption{
				Concurrency: state.LastWrite,
			},
			ETag: "dragon",
		})
		assert.Equal(t, nil, err, "failed to parse ETag")
		assert.Equal(t, 0, ver, "version should be 0")
	})
}

func TestParseConnectedSlavs(t *testing.T) {
	store := NewRedisStateStore(logger.NewLogger("test"))

	t.Run("Empty info", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("")
		assert.Equal(t, 0, slaves, "connected slaves must be 0")
	})

	t.Run("connectedSlaves property is not included", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("# Replication\r\nrole:master\r\n")
		assert.Equal(t, 0, slaves, "connected slaves must be 0")
	})

	t.Run("connectedSlaves is 2", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("# Replication\r\nrole:master\r\nconnected_slaves:2\r\n")
		assert.Equal(t, 2, slaves, "connected slaves must be 2")
	})

	t.Run("connectedSlaves is 1", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("# Replication\r\nrole:master\r\nconnected_slaves:1")
		assert.Equal(t, 1, slaves, "connected slaves must be 1")
	})
}

func TestUpsertWithEtag(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	ss := &StateStore{
		client: c,
		json:   jsoniter.ConfigFastest,
		logger: logger.NewLogger("test"),
	}

	key := fmt.Sprintf("key-%d", time.Now().UnixNano())
	tag := ""

	t.Run("Set initial value", func(t *testing.T) {
		if err := ss.Set(&state.SetRequest{
			Key:   key,
			Value: "initial value",
			Options: state.SetStateOption{
				Concurrency: "first-write",
				Consistency: "strong",
			},
		}); err != nil {
			t.Fatalf("error on initial value set: %v", err)
		}
	})

	t.Run("Get initial value", func(t *testing.T) {
		item, err := ss.Get(&state.GetRequest{
			Key: key,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		})
		if err != nil {
			t.Fatalf("error on initial value get: %v", err)
		}
		assert.Equal(t, "\"initial value\"", string(item.Data))
		assert.Equal(t, "1", item.ETag)
	})

	t.Run("Set invalid etag value", func(t *testing.T) {
		if err := ss.Set(&state.SetRequest{
			Key:   key,
			Value: "new value",
			ETag:  "100",
			Options: state.SetStateOption{
				Concurrency: "first-write",
				Consistency: "strong",
			},
		}); err == nil {
			t.Fatalf("expected error on set with an invalid etag")
		}
	})

	t.Run("Get after invalid etag set", func(t *testing.T) {
		item, err := ss.Get(&state.GetRequest{
			Key: key,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		})
		if err != nil {
			t.Fatalf("error on updated value get: %v", err)
		}
		assert.Equal(t, "\"initial value\"", string(item.Data))
		tag = item.ETag
	})

	t.Run("Set multiple with invalid etag", func(t *testing.T) {
		if err := ss.Multi(&state.TransactionalStateRequest{
			Operations: []state.TransactionalStateOperation{
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   key,
						Value: "invalid update",
						ETag:  "200",
						Options: state.SetStateOption{
							Concurrency: "first-write",
							Consistency: "strong",
						},
					},
				},
				{
					Operation: state.Upsert,
					Request: state.SetRequest{
						Key:   key,
						Value: "valid update but should fail due to the previous set",
						ETag:  tag,
						Options: state.SetStateOption{
							Concurrency: "first-write",
							Consistency: "strong",
						},
					},
				},
			},
		}); err == nil {
			t.Fatalf("expected error on multi set with an invalid etag")
		}
	})

	t.Run("Get after multiple with invalid etag", func(t *testing.T) {
		item3, err := ss.Get(&state.GetRequest{
			Key: key,
			Options: state.GetStateOption{
				Consistency: "strong",
			},
		})
		if err != nil {
			t.Fatalf("error on get final upserted value get: %v", err)
		}
		assert.Equal(t, "\"initial value\"", string(item3.Data))
	})

	t.Run("Get after multiple with invalid etag", func(t *testing.T) {
		if err := ss.Delete(&state.DeleteRequest{
			Key:  key,
			ETag: tag,
		}); err != nil {
			t.Fatalf("error on updated value get: %v", err)
		}
	})
}

func TestTransactionalUpsert(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	ss := &StateStore{
		client: c,
		json:   jsoniter.ConfigFastest,
		logger: logger.NewLogger("test"),
	}

	err := ss.Multi(&state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{{
			Operation: state.Upsert,
			Request: state.SetRequest{
				Key:   "weapon",
				Value: "deathstar",
			},
		}},
	})
	assert.Equal(t, nil, err)

	res, err := c.DoContext(context.Background(), "HGETALL", "weapon").Result()
	assert.Equal(t, nil, err)

	vals := res.([]interface{})
	data, version, err := ss.getKeyVersion(vals)
	assert.Equal(t, nil, err)
	assert.Equal(t, "1", version)
	assert.Equal(t, `"deathstar"`, data)
}

func TestTransactionalDelete(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	ss := &StateStore{
		client: c,
		json:   jsoniter.ConfigFastest,
		logger: logger.NewLogger("test"),
	}

	// Insert a record first.
	ss.Set(&state.SetRequest{
		Key:   "weapon",
		Value: "deathstar",
	})

	err := ss.Multi(&state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{{
			Operation: state.Delete,
			Request: state.DeleteRequest{
				Key:  "weapon",
				ETag: "1",
			},
		}},
	})
	assert.Equal(t, nil, err)

	res, err := c.DoContext(context.Background(), "HGETALL", "weapon").Result()
	assert.Equal(t, nil, err)

	vals := res.([]interface{})
	assert.Equal(t, 0, len(vals))
}

func TestTransactionalDeleteNoEtag(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	ss := &StateStore{
		client: c,
		json:   jsoniter.ConfigFastest,
		logger: logger.NewLogger("test"),
	}

	// Insert a record first.
	ss.Set(&state.SetRequest{
		Key:   "weapon100",
		Value: "deathstar100",
	})

	err := ss.Multi(&state.TransactionalStateRequest{
		Operations: []state.TransactionalStateOperation{{
			Operation: state.Delete,
			Request: state.DeleteRequest{
				Key: "weapon100",
			},
		}},
	})
	assert.Equal(t, nil, err)

	res, err := c.DoContext(context.Background(), "HGETALL", "weapon100").Result()
	assert.Equal(t, nil, err)

	vals := res.([]interface{})
	assert.Equal(t, 0, len(vals))
}

func setupMiniredis() (*miniredis.Miniredis, *redis.Client) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	opts := &redis.Options{
		Addr: s.Addr(),
		DB:   defaultDB,
	}

	return s, redis.NewClient(opts)
}
