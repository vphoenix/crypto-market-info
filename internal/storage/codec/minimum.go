package codec

// The smallest valid representation is checked before allocating an array from
// untrusted length fields. A short corrupt payload cannot allocate a huge slice.
func minimumSize(t *Type) int {
	switch t.Name {
	case "Nullable", "Bool", "Enum8", "UInt8":
		return 1
	case "UInt16":
		return 2
	case "Array", "String", "UInt32", "Int32":
		return 4
	case "UInt64", "Int64", "DateTime", "DateTime64":
		return 8
	case "UInt256":
		return 32
	case "Decimal", "UUID":
		return 16
	case "FixedString":
		return t.Size
	case "LowCardinality":
		return minimumSize(t.Children[0])
	case "Tuple":
		n := 0
		for _, c := range t.Children {
			n += minimumSize(c)
		}
		return n
	}
	return 1
}
