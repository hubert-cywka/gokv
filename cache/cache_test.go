package cache

import (
	"bytes"
	"errors"
	"testing"
)

func TestCache_Get(t *testing.T) {
	options := Options{
		InitialCapacity: 512,
	}

	t.Run("returns value if key exists", func(t *testing.T) {
		c := NewCache(options)
		key, want := "key", []byte("value")

		_ = c.Set(key, want)
		got, err := c.Get(key)

		assertNoError(t, err)
		assertValue(t, got, want)
	})

	t.Run("returns ErrNotFound if key does not exist", func(t *testing.T) {
		c := NewCache(options)

		_, err := c.Get("non-existent")

		if !errors.Is(err, ErrNotFound) {
			t.Errorf("got error %v, want %v", err, ErrNotFound)
		}
	})
}

func TestCache_Set(t *testing.T) {
	options := Options{
		InitialCapacity: 512,
	}
	t.Run("sets and overwrites values", func(t *testing.T) {
		c := NewCache(options)
		key := "key"
		val1, val2 := []byte("v1"), []byte("v2")

		_ = c.Set(key, val1)
		err := c.Set(key, val2)

		got, _ := c.Get(key)

		assertNoError(t, err)
		assertValue(t, got, val2)
	})
}

func TestCache_Delete(t *testing.T) {
	options := Options{
		InitialCapacity: 512,
	}
	t.Run("removes value from storage", func(t *testing.T) {
		c := NewCache(options)
		key, val := "key", []byte("value")

		_ = c.Set(key, val)
		err := c.Delete(key)
		assertNoError(t, err)

		_, err = c.Get(key)
		if !errors.Is(err, ErrNotFound) {
			t.Error("expected key to be deleted, but it was still found")
		}
	})
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertValue(t *testing.T, got, want []byte) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
