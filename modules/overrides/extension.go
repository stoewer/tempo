package overrides

import (
	"encoding/json"
	"flag"
	"fmt"
	"reflect"
	"sync"
)

// Extension describes a typed extension to the per-tenant overrides config.
// Implementations must use pointer receivers for all methods.
type Extension interface {
	// Key is the YAML/JSON property name used to store this extension's config.
	Key() string
	// RegisterFlagsAndApplyDefaults applies defaults for the extension config.
	RegisterFlagsAndApplyDefaults(prefix string, f *flag.FlagSet)
	// Validate validates the extension config after it has been decoded.
	Validate() error
	// LegacyKeys returns the flat-key names used in the legacy overrides format.
	// Return an empty slice if there are no legacy keys.
	LegacyKeys() []string
	// FromLegacy populates this extension from the flat legacy key map.
	// The full Extra map is passed; implementations pick only their own keys.
	FromLegacy(map[string]any) error
	// ToLegacy serializes this extension to the flat legacy key map.
	ToLegacy() map[string]any
}

// registryEntry holds the reflect metadata needed to instantiate an extension.
type registryEntry struct {
	key        string
	legacyKeys []string
	elemType   reflect.Type // struct type (T without pointer indirection)
}

// newInstance creates a zeroed pointer instance of the extension type, cast to Extension.
func (e *registryEntry) newInstance() Extension {
	return reflect.New(e.elemType).Interface().(Extension)
}

var extensionRegistry = struct {
	sync.RWMutex
	entries map[string]*registryEntry
}{entries: make(map[string]*registryEntry)}

// RegisterExtension registers a per-tenant overrides extension.
// e must be a non-nil pointer to the extension struct (pointer receivers are required).
// Panics if an extension with the same Key() is already registered.
// Returns a typed getter that retrieves the extension value from an Overrides.
func RegisterExtension[T Extension](e T) func(*Overrides) T {
	key := e.Key()

	extensionRegistry.Lock()
	defer extensionRegistry.Unlock()

	if _, exists := extensionRegistry.entries[key]; exists {
		panic(fmt.Sprintf("overrides: extension %q already registered", key))
	}

	typ := reflect.TypeOf(e)
	if typ == nil || typ.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("overrides: extension %q must be registered as a pointer type", key))
	}

	extensionRegistry.entries[key] = &registryEntry{
		key:        key,
		legacyKeys: e.LegacyKeys(),
		elemType:   typ.Elem(),
	}

	return func(o *Overrides) T {
		if o == nil || o.Extra == nil {
			var zero T
			return zero
		}
		v, _ := o.Extra[key].(T)
		return v
	}
}

// processExtensions validates all entries in o.Extra against the registry, converts raw
// decoded values (from YAML or JSON) to typed Extension instances, applies defaults, and
// calls Validate on each. It is idempotent: already-typed entries are only re-validated.
func processExtensions(o *Overrides) error {
	if len(o.Extra) == 0 {
		return nil
	}

	extensionRegistry.RLock()
	entries := make(map[string]*registryEntry, len(extensionRegistry.entries))
	for k, v := range extensionRegistry.entries {
		entries[k] = v
	}
	extensionRegistry.RUnlock()

	for key, raw := range o.Extra {
		entry, ok := entries[key]
		if !ok {
			return fmt.Errorf("unknown extension key %q: must be registered via RegisterExtension before use", key)
		}

		// Already a typed Extension (e.g., set programmatically or after legacy conversion): just validate.
		if ext, alreadyTyped := raw.(Extension); alreadyTyped {
			if err := ext.Validate(); err != nil {
				return fmt.Errorf("extension %q: %w", key, err)
			}
			continue
		}

		// Create a new instance and apply defaults.
		instance := entry.newInstance()
		instance.RegisterFlagsAndApplyDefaults("", flag.NewFlagSet("", flag.ContinueOnError))

		// Decode via JSON round-trip, which also normalises map[interface{}]interface{} from YAML.
		b, err := json.Marshal(normalizeYAMLValue(raw))
		if err != nil {
			return fmt.Errorf("extension %q: marshal: %w", key, err)
		}
		if err := json.Unmarshal(b, instance); err != nil {
			return fmt.Errorf("extension %q: unmarshal: %w", key, err)
		}

		if err := instance.Validate(); err != nil {
			return fmt.Errorf("extension %q: %w", key, err)
		}

		o.Extra[key] = instance
	}
	return nil
}

// allExtensionFlatKeys returns the union of all LegacyKeys across all registered extensions.
func allExtensionFlatKeys() map[string]struct{} {
	extensionRegistry.RLock()
	defer extensionRegistry.RUnlock()

	out := make(map[string]struct{})
	for _, e := range extensionRegistry.entries {
		for _, k := range e.legacyKeys {
			out[k] = struct{}{}
		}
	}
	return out
}

// normalizeYAMLValue converts map[interface{}]interface{} produced by go-yaml to
// map[string]any recursively, making the value safe to pass to json.Marshal.
func normalizeYAMLValue(v any) any {
	switch val := v.(type) {
	case map[interface{}]interface{}:
		m := make(map[string]any, len(val))
		for k, v2 := range val {
			m[fmt.Sprintf("%v", k)] = normalizeYAMLValue(v2)
		}
		return m
	case []interface{}:
		for i, elem := range val {
			val[i] = normalizeYAMLValue(elem)
		}
		return val
	default:
		return v
	}
}

// ResetRegistryForTesting clears the extension registry and restores it after the test.
// This prevents extension registrations in one test from leaking into others.
func ResetRegistryForTesting(t interface{ Cleanup(func()) }) {
	extensionRegistry.Lock()
	saved := extensionRegistry.entries
	extensionRegistry.entries = make(map[string]*registryEntry)
	extensionRegistry.Unlock()

	t.Cleanup(func() {
		extensionRegistry.Lock()
		extensionRegistry.entries = saved
		extensionRegistry.Unlock()
	})
}
