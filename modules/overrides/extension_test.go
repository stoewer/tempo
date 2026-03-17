package overrides

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v2"
)

// ---------------------------------------------------------------------------
// testExtension — a realistic extension used across all extension tests.
// It implements Extension via pointer receivers and has both a nested-key
// (new format) and flat-key (legacy format) representation.
// ---------------------------------------------------------------------------

var _ Extension = (*testExtension)(nil)

type testExtension struct {
	FieldA string `yaml:"field_a" json:"field_a"`
	FieldB *int   `yaml:"field_b" json:"field_b"`
}

func (t *testExtension) Key() string { return "test_extension" }

func (t *testExtension) RegisterFlagsAndApplyDefaults(_ string, _ *flag.FlagSet) {
	t.FieldA = "field_a_default"
	t.FieldB = new(int) // default: pointer to 0
}

func (t *testExtension) Validate() error {
	if t.FieldA == "" {
		return fmt.Errorf("field_a cannot be empty")
	}
	if t.FieldB == nil {
		return fmt.Errorf("field_b cannot be nil")
	}
	return nil
}

func (t *testExtension) LegacyKeys() []string {
	return []string{"test_extension_field_a", "test_extension_field_b"}
}

func (t *testExtension) FromLegacy(m map[string]any) error {
	if v, ok := m["test_extension_field_a"].(string); ok {
		t.FieldA = v
	}
	if v, ok := m["test_extension_field_b"].(int); ok {
		t.FieldB = &v
	}
	return nil
}

func (t *testExtension) ToLegacy() map[string]any {
	if t == nil {
		return nil
	}
	fieldB := 0
	if t.FieldB != nil {
		fieldB = *t.FieldB
	}
	return map[string]any{
		"test_extension_field_a": t.FieldA,
		"test_extension_field_b": fieldB,
	}
}

// ---------------------------------------------------------------------------
// Registration tests
// ---------------------------------------------------------------------------

func TestRegisterExtension_PanicsOnDuplicate(t *testing.T) {
	ResetRegistryForTesting(t)
	RegisterExtension[*testExtension](&testExtension{})
	assert.Panics(t, func() { RegisterExtension[*testExtension](&testExtension{}) })
}

func TestRegisterExtension_TypedGetter(t *testing.T) {
	ResetRegistryForTesting(t)
	get := RegisterExtension[*testExtension](&testExtension{})

	fieldB := 99
	o := Overrides{
		Extra: map[string]any{"test_extension": &testExtension{FieldA: "hello", FieldB: &fieldB}},
	}
	ext := get(&o)
	require.NotNil(t, ext)
	assert.Equal(t, "hello", ext.FieldA)
	assert.Equal(t, 99, *ext.FieldB)

	// Nil Overrides and empty Extra both return zero value.
	assert.Nil(t, get(nil))
	assert.Nil(t, get(&Overrides{}))
}

// ---------------------------------------------------------------------------
// MarshalJSON tests
// ---------------------------------------------------------------------------

func TestOverridesExtension_MarshalJSON(t *testing.T) {
	t.Run("Overrides", func(t *testing.T) {
		ResetRegistryForTesting(t)
		get := RegisterExtension[*testExtension](&testExtension{})

		fieldB := 42
		o := Overrides{}
		o.Ingestion.MaxLocalTracesPerUser = 1000
		o.Extra = map[string]any{
			"test_extension": &testExtension{FieldA: "custom", FieldB: &fieldB},
		}

		b, err := json.Marshal(&o)
		require.NoError(t, err)

		var o2 Overrides
		require.NoError(t, json.Unmarshal(b, &o2))

		assert.Equal(t, 1000, o2.Ingestion.MaxLocalTracesPerUser)
		ext := get(&o2)
		require.NotNil(t, ext)
		assert.Equal(t, "custom", ext.FieldA)
		assert.Equal(t, 42, *ext.FieldB)
	})

	t.Run("LegacyOverrides", func(t *testing.T) {
		ResetRegistryForTesting(t)
		RegisterExtension[*testExtension](&testExtension{})

		fieldB := 7
		o := Overrides{}
		o.Ingestion.MaxLocalTracesPerUser = 500
		o.Extra = map[string]any{
			"test_extension": &testExtension{FieldA: "flat_val", FieldB: &fieldB},
		}
		l := o.toLegacy()

		b, err := json.Marshal(&l)
		require.NoError(t, err)

		// The nested extension key must be replaced by its flat keys in JSON.
		var m map[string]any
		require.NoError(t, json.Unmarshal(b, &m))
		assert.Equal(t, "flat_val", m["test_extension_field_a"], "flat key must appear in JSON")
		assert.Nil(t, m["test_extension"], "nested key must not appear in JSON")

		// Unmarshal the JSON back to LegacyOverrides; flat keys land in Extra.
		var l2 LegacyOverrides
		require.NoError(t, json.Unmarshal(b, &l2))
		assert.Equal(t, "flat_val", l2.Extra["test_extension_field_a"])
	})
}

// ---------------------------------------------------------------------------
// UnmarshalJSON tests
// ---------------------------------------------------------------------------

func TestOverridesExtension_UnmarshalJSON(t *testing.T) {
	t.Run("Overrides", func(t *testing.T) {
		ResetRegistryForTesting(t)
		get := RegisterExtension[*testExtension](&testExtension{})

		input := `{
			"ingestion": {"max_traces_per_user": 1000},
			"test_extension": {"field_a": "from_json", "field_b": 55}
		}`
		var o Overrides
		require.NoError(t, json.Unmarshal([]byte(input), &o))

		assert.Equal(t, 1000, o.Ingestion.MaxLocalTracesPerUser)
		ext := get(&o)
		require.NotNil(t, ext)
		assert.Equal(t, "from_json", ext.FieldA)
		assert.Equal(t, 55, *ext.FieldB)
	})

	t.Run("Overrides_defaults_applied", func(t *testing.T) {
		ResetRegistryForTesting(t)
		get := RegisterExtension[*testExtension](&testExtension{})

		// field_b is omitted — the default (pointer to 0) must be used.
		input := `{"test_extension": {"field_a": "only_a"}}`
		var o Overrides
		require.NoError(t, json.Unmarshal([]byte(input), &o))

		ext := get(&o)
		require.NotNil(t, ext)
		assert.Equal(t, "only_a", ext.FieldA)
		require.NotNil(t, ext.FieldB, "default FieldB must be applied")
		assert.Equal(t, 0, *ext.FieldB)
	})

	t.Run("Overrides_unregistered_key_errors", func(t *testing.T) {
		ResetRegistryForTesting(t)

		input := `{"ingestion": {"max_traces_per_user": 1}, "unknown_ext": {"x": 1}}`
		var o Overrides
		err := json.Unmarshal([]byte(input), &o)
		require.ErrorContains(t, err, "unknown extension key")
	})

	t.Run("Overrides_validate_error", func(t *testing.T) {
		ResetRegistryForTesting(t)
		RegisterExtension[*testExtension](&testExtension{})

		// field_a is empty — Validate must reject it.
		input := `{"test_extension": {"field_a": ""}}`
		var o Overrides
		err := json.Unmarshal([]byte(input), &o)
		require.ErrorContains(t, err, "field_a cannot be empty")
	})

	t.Run("LegacyOverrides", func(t *testing.T) {
		ResetRegistryForTesting(t)
		RegisterExtension[*testExtension](&testExtension{})

		// LegacyOverrides JSON may contain flat extension keys; they must land in Extra
		// without error (processing is deferred to toNewLimits).
		input := `{"max_traces_per_user": 1000, "test_extension_field_a": "from_legacy_json"}`
		var l LegacyOverrides
		require.NoError(t, json.Unmarshal([]byte(input), &l))
		assert.Equal(t, 1000, l.MaxLocalTracesPerUser)
		assert.Equal(t, "from_legacy_json", l.Extra["test_extension_field_a"])
	})
}

// ---------------------------------------------------------------------------
// MarshalYAML tests
// ---------------------------------------------------------------------------

func TestOverridesExtension_MarshalYAML(t *testing.T) {
	t.Run("Overrides", func(t *testing.T) {
		ResetRegistryForTesting(t)
		get := RegisterExtension[*testExtension](&testExtension{})

		fieldB := 3
		o := Overrides{}
		o.Ingestion.MaxLocalTracesPerUser = 1000
		o.Extra = map[string]any{
			"test_extension": &testExtension{FieldA: "yaml_val", FieldB: &fieldB},
		}

		b, err := yaml.Marshal(&o)
		require.NoError(t, err)

		// The marshaled YAML must contain the nested extension key.
		assert.Contains(t, string(b), "test_extension:")

		// Round-trip: unmarshal + processExtensions must recover the extension.
		var o2 Overrides
		require.NoError(t, yaml.Unmarshal(b, &o2))
		require.NoError(t, processExtensions(&o2))

		assert.Equal(t, 1000, o2.Ingestion.MaxLocalTracesPerUser)
		ext := get(&o2)
		require.NotNil(t, ext)
		assert.Equal(t, "yaml_val", ext.FieldA)
		assert.Equal(t, 3, *ext.FieldB)
	})

	t.Run("LegacyOverrides", func(t *testing.T) {
		ResetRegistryForTesting(t)
		RegisterExtension[*testExtension](&testExtension{})

		fieldB := 8
		o := Overrides{}
		o.Ingestion.MaxLocalTracesPerUser = 500
		o.Extra = map[string]any{
			"test_extension": &testExtension{FieldA: "legacy_yaml", FieldB: &fieldB},
		}
		l := o.toLegacy()

		b, err := yaml.Marshal(&l)
		require.NoError(t, err)

		// Flat keys must appear at the top level; nested key must not.
		yamlStr := string(b)
		assert.Contains(t, yamlStr, "test_extension_field_a:")
		assert.NotContains(t, yamlStr, "test_extension:")
	})
}

// ---------------------------------------------------------------------------
// UnmarshalYAML tests
// ---------------------------------------------------------------------------

func TestOverridesExtension_UnmarshalYAML(t *testing.T) {
	t.Run("Overrides", func(t *testing.T) {
		ResetRegistryForTesting(t)
		get := RegisterExtension[*testExtension](&testExtension{})

		input := `
ingestion:
  max_traces_per_user: 1000
test_extension:
  field_a: from_yaml
  field_b: 11
`
		var o Overrides
		require.NoError(t, yaml.Unmarshal([]byte(input), &o))
		// After YAML unmarshal, Extra holds a raw map; processExtensions decodes it.
		require.NoError(t, processExtensions(&o))

		assert.Equal(t, 1000, o.Ingestion.MaxLocalTracesPerUser)
		ext := get(&o)
		require.NotNil(t, ext)
		assert.Equal(t, "from_yaml", ext.FieldA)
		assert.Equal(t, 11, *ext.FieldB)
	})

	t.Run("Overrides_strict_decoder_absorbs_extension_key", func(t *testing.T) {
		ResetRegistryForTesting(t)
		RegisterExtension[*testExtension](&testExtension{})

		input := `
ingestion:
  max_traces_per_user: 500
test_extension:
  field_a: strict_ok
`
		var o Overrides
		decoder := yaml.NewDecoder(strings.NewReader(input))
		decoder.SetStrict(true)
		require.NoError(t, decoder.Decode(&o), "strict YAML decoder must not error on registered extension keys")
	})

	t.Run("Overrides_unregistered_key_errors", func(t *testing.T) {
		ResetRegistryForTesting(t)

		input := `
ingestion:
  max_traces_per_user: 1
unknown_ext:
  x: 1
`
		var o Overrides
		require.NoError(t, yaml.Unmarshal([]byte(input), &o))
		err := processExtensions(&o)
		require.ErrorContains(t, err, "unknown extension key")
	})

	t.Run("LegacyOverrides", func(t *testing.T) {
		ResetRegistryForTesting(t)
		get := RegisterExtension[*testExtension](&testExtension{})

		input := `
max_traces_per_user: 1000
test_extension_field_a: from_legacy_yaml
`
		var l LegacyOverrides
		require.NoError(t, yaml.Unmarshal([]byte(input), &l))

		assert.Equal(t, 1000, l.MaxLocalTracesPerUser)
		assert.Equal(t, "from_legacy_yaml", l.Extra["test_extension_field_a"])

		// Convert to new format: flat keys become a typed extension.
		o, err := l.toNewLimits()
		require.NoError(t, err)
		require.NoError(t, processExtensions(&o))

		ext := get(&o)
		require.NotNil(t, ext)
		assert.Equal(t, "from_legacy_yaml", ext.FieldA)
		// field_b was not in the YAML; the default (pointer to 0) must be used.
		require.NotNil(t, ext.FieldB)
		assert.Equal(t, 0, *ext.FieldB)
	})
}

// ---------------------------------------------------------------------------
// Full round-trip tests
// ---------------------------------------------------------------------------

func TestExtension_FullLegacyRoundTrip_perTenantOverrides(t *testing.T) {
	ResetRegistryForTesting(t)
	get := RegisterExtension[*testExtension](&testExtension{})

	input := `
overrides:
  tenant-1:
    max_traces_per_user: 1000
    test_extension_field_a: roundtrip_val
`
	var pto perTenantOverrides
	decoder := yaml.NewDecoder(strings.NewReader(input))
	decoder.SetStrict(true)
	require.NoError(t, decoder.Decode(&pto))
	require.Equal(t, ConfigTypeLegacy, pto.ConfigType)

	limits := pto.TenantLimits["tenant-1"]
	require.NotNil(t, limits)
	assert.Equal(t, 1000, limits.Ingestion.MaxLocalTracesPerUser)
	ext := get(limits)
	require.NotNil(t, ext)
	assert.Equal(t, "roundtrip_val", ext.FieldA)
}

func TestExtension_FullNewFormatRoundTrip_perTenantOverrides(t *testing.T) {
	ResetRegistryForTesting(t)
	get := RegisterExtension[*testExtension](&testExtension{})

	input := `
overrides:
  tenant-1:
    ingestion:
      max_traces_per_user: 2000
    test_extension:
      field_a: new_format_val
      field_b: 5
`
	var pto perTenantOverrides
	decoder := yaml.NewDecoder(strings.NewReader(input))
	decoder.SetStrict(true)
	require.NoError(t, decoder.Decode(&pto))
	require.Equal(t, ConfigTypeNew, pto.ConfigType)

	limits := pto.TenantLimits["tenant-1"]
	require.NotNil(t, limits)
	assert.Equal(t, 2000, limits.Ingestion.MaxLocalTracesPerUser)
	ext := get(limits)
	require.NotNil(t, ext)
	assert.Equal(t, "new_format_val", ext.FieldA)
	assert.Equal(t, 5, *ext.FieldB)
}

func TestExtension_JSONRoundTrip_Overrides(t *testing.T) {
	ResetRegistryForTesting(t)
	get := RegisterExtension[*testExtension](&testExtension{})

	fieldB := 13
	o := Overrides{}
	o.Ingestion.MaxLocalTracesPerUser = 777
	o.Extra = map[string]any{
		"test_extension": &testExtension{FieldA: "json_rt", FieldB: &fieldB},
	}

	b, err := json.Marshal(&o)
	require.NoError(t, err)

	var o2 Overrides
	require.NoError(t, json.Unmarshal(b, &o2))

	assert.Equal(t, 777, o2.Ingestion.MaxLocalTracesPerUser)
	ext := get(&o2)
	require.NotNil(t, ext)
	assert.Equal(t, "json_rt", ext.FieldA)
	assert.Equal(t, 13, *ext.FieldB)
}

func TestExtension_LegacyConversionRoundTrip(t *testing.T) {
	ResetRegistryForTesting(t)
	get := RegisterExtension[*testExtension](&testExtension{})

	fieldB := 21
	// Build an Overrides with a typed extension, convert to legacy and back.
	o := Overrides{}
	o.Extra = map[string]any{
		"test_extension": &testExtension{FieldA: "converted", FieldB: &fieldB},
	}
	l := o.toLegacy()

	assert.Equal(t, "converted", l.Extra["test_extension_field_a"])
	assert.Nil(t, l.Extra["test_extension"], "nested key must not appear in legacy")

	o2, err := l.toNewLimits()
	require.NoError(t, err)
	require.NoError(t, processExtensions(&o2))

	ext := get(&o2)
	require.NotNil(t, ext)
	assert.Equal(t, "converted", ext.FieldA)
	assert.Equal(t, 21, *ext.FieldB)
}

