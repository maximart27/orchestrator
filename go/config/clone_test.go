package config

import (
	"encoding/json"
	"reflect"
	"testing"
)

// fillNonZero gives every field a non-zero value so that aliasing between a configuration and its clone
// shows up for every kind of field.
func fillNonZero(t *testing.T, c *Configuration) {
	t.Helper()
	v := reflect.ValueOf(c).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.Bool:
			f.SetBool(true)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			f.SetInt(7)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			f.SetUint(7)
		case reflect.Float32, reflect.Float64:
			f.SetFloat(7)
		case reflect.String:
			f.SetString("value-" + v.Type().Field(i).Name)
		case reflect.Slice:
			if f.Type().Elem().Kind() != reflect.String {
				t.Fatalf("field %s: unsupported slice type %s, extend fillNonZero/mutateInPlace", v.Type().Field(i).Name, f.Type())
			}
			f.Set(reflect.ValueOf([]string{"one", "two"}))
		case reflect.Map:
			if f.Type() != reflect.TypeOf(map[string]string{}) {
				t.Fatalf("field %s: unsupported map type %s, extend fillNonZero/mutateInPlace", v.Type().Field(i).Name, f.Type())
			}
			f.Set(reflect.ValueOf(map[string]string{"k1": "v1", "k2": "v2"}))
		default:
			t.Fatalf("field %s: unsupported kind %s, extend fillNonZero/mutateInPlace", v.Type().Field(i).Name, f.Kind())
		}
	}
}

// mutateInPlace modifies a field the way that would leak into another configuration if memory was shared.
func mutateInPlace(f reflect.Value) {
	switch f.Kind() {
	case reflect.Bool:
		f.SetBool(!f.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		f.SetInt(f.Int() + 1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		f.SetUint(f.Uint() + 1)
	case reflect.Float32, reflect.Float64:
		f.SetFloat(f.Float() + 1)
	case reflect.String:
		f.SetString(f.String() + "-mutated")
	case reflect.Slice:
		for i := 0; i < f.Len(); i++ {
			f.Index(i).SetString("mutated")
		}
	case reflect.Map:
		for _, key := range f.MapKeys() {
			f.SetMapIndex(key, reflect.ValueOf("mutated"))
		}
		f.SetMapIndex(reflect.ValueOf("added"), reflect.ValueOf("added"))
	}
}

func mustJSON(t *testing.T, c *Configuration) string {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestCloneEqualsOriginal(t *testing.T) {
	filled := newConfiguration()
	fillNonZero(t, filled)
	for name, c := range map[string]*Configuration{"defaults": newConfiguration(), "filled": filled, "zero": {}} {
		clone := c.Clone()
		if clone == c {
			t.Fatalf("%s: Clone returned the same pointer", name)
		}
		if !reflect.DeepEqual(c, clone) {
			t.Errorf("%s: clone differs from original (nil vs empty slices/maps must be preserved)", name)
		}
	}
	var nilConfig *Configuration
	if nilConfig.Clone() != nil {
		t.Errorf("Clone of nil must be nil")
	}
}

// TestCloneSharesNoMemory mutates, one field at a time, the clone and checks that the original is untouched.
func TestCloneSharesNoMemory(t *testing.T) {
	orig := newConfiguration()
	fillNonZero(t, orig)
	snapshot := mustJSON(t, orig)

	typ := reflect.TypeOf(*orig)
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		t.Run(name, func(t *testing.T) {
			clone := orig.Clone()
			mutateInPlace(reflect.ValueOf(clone).Elem().Field(i))
			if after := mustJSON(t, orig); after != snapshot {
				t.Fatalf("mutating clone.%s altered the original", name)
			}
			if reflect.DeepEqual(orig, clone) {
				t.Fatalf("mutation of clone.%s had no effect (test is not exercising the field)", name)
			}
		})
	}

	// and the other way around: mutating the original must not alter a clone
	clone := orig.Clone()
	cloneSnapshot := mustJSON(t, clone)
	v := reflect.ValueOf(orig).Elem()
	for i := 0; i < v.NumField(); i++ {
		mutateInPlace(v.Field(i))
	}
	if mustJSON(t, clone) != cloneSnapshot {
		t.Fatalf("mutating the original altered an existing clone")
	}
}
