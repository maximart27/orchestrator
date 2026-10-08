package config

import "reflect"

// Clone returns a deep copy of the configuration: slices, maps and pointers are
// duplicated, so mutating the clone never alters the receiver (and vice versa).
// nil and empty slices/maps keep their distinction. The copy is reflection based,
// so fields added to Configuration in the future are covered automatically.
func (this *Configuration) Clone() *Configuration {
	if this == nil {
		return nil
	}
	clone := new(Configuration)
	deepCopyValue(reflect.ValueOf(clone).Elem(), reflect.ValueOf(this).Elem())
	return clone
}

func deepCopyValue(dst, src reflect.Value) {
	switch src.Kind() {
	case reflect.Ptr:
		if src.IsNil() {
			return
		}
		dst.Set(reflect.New(src.Type().Elem()))
		deepCopyValue(dst.Elem(), src.Elem())
	case reflect.Slice:
		if src.IsNil() {
			return
		}
		dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Len()))
		for i := 0; i < src.Len(); i++ {
			deepCopyValue(dst.Index(i), src.Index(i))
		}
	case reflect.Array:
		for i := 0; i < src.Len(); i++ {
			deepCopyValue(dst.Index(i), src.Index(i))
		}
	case reflect.Map:
		if src.IsNil() {
			return
		}
		dst.Set(reflect.MakeMapWithSize(src.Type(), src.Len()))
		iter := src.MapRange()
		for iter.Next() {
			value := reflect.New(src.Type().Elem()).Elem()
			deepCopyValue(value, iter.Value())
			dst.SetMapIndex(iter.Key(), value)
		}
	case reflect.Struct:
		for i := 0; i < src.NumField(); i++ {
			deepCopyValue(dst.Field(i), src.Field(i))
		}
	case reflect.Interface:
		if src.IsNil() {
			return
		}
		value := reflect.New(src.Elem().Type()).Elem()
		deepCopyValue(value, src.Elem())
		dst.Set(value)
	default:
		dst.Set(src)
	}
}
