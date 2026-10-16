// Package codec defines the versioned, lossless storage representation. It has
// no SQL or exchange dependencies. Unsupported types fail closed.
package codec

import (
	"crypto/sha256"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const Version byte = 1
const MaxBytes = 16 << 20
const MaxElements = 1 << 16

type Type struct {
	Name     string
	Text     string
	Size     int
	Scale    int
	Children []*Type
	Fields   []string
	Enums    map[string]int8
	Tag      [16]byte
}

// Split separates top-level arguments without splitting quoted names or nested
// arrays/tuples. Unbalanced or unterminated declarations are rejected.
func Split(s string) ([]string, error) {
	var out []string
	start, depth := 0, 0
	quoted, escaped := false, false
	for i, c := range s {
		if escaped {
			escaped = false
			continue
		}
		if quoted && c == '\\' {
			escaped = true
			continue
		}
		if c == '\'' {
			quoted = !quoted
			continue
		}
		if quoted {
			continue
		}
		switch c {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced type")
			}
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if depth != 0 || quoted || escaped {
		return nil, fmt.Errorf("unbalanced type")
	}
	out = append(out, strings.TrimSpace(s[start:]))
	return out, nil
}

var enumPattern = regexp.MustCompile(`^'([a-zA-Z0-9_]+)'\s*=\s*(-?[0-9]+)$`)

func Parse(text string) (*Type, error) { return parse(strings.TrimSpace(text), 0) }
func parse(text string, depth int) (*Type, error) {
	if depth > 16 || len(text) > 8192 {
		return nil, fmt.Errorf("type too complex")
	}
	t := &Type{Text: text, Name: text}
	h := sha256.Sum256([]byte(text))
	copy(t.Tag[:], h[:16])
	var args []string
	if i := strings.IndexByte(text, '('); i >= 0 {
		if !strings.HasSuffix(text, ")") {
			return nil, fmt.Errorf("invalid type %q", text)
		}
		t.Name = text[:i]
		var err error
		args, err = Split(text[i+1 : len(text)-1])
		if err != nil {
			return nil, err
		}
	}
	child := func(s string) error {
		c, e := parse(s, depth+1)
		if e == nil {
			t.Children = append(t.Children, c)
		}
		return e
	}
	switch t.Name {
	case "UInt8", "UInt16", "UInt32", "UInt64", "Int32", "Int64", "UInt256", "UUID", "Bool", "String":
		if len(args) != 0 {
			return nil, fmt.Errorf("unexpected type arguments")
		}
	case "FixedString":
		if len(args) != 1 {
			return nil, fmt.Errorf("fixed string needs length")
		}
		n, e := strconv.Atoi(args[0])
		if e != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("invalid fixed length")
		}
		t.Size = n
	case "Decimal":
		if len(args) != 2 || args[0] != "38" || args[1] != "18" {
			return nil, fmt.Errorf("unsupported decimal %q", text)
		}
		t.Size = 38
		t.Scale = 18
	case "DateTime", "DateTime64":
		if t.Name == "DateTime" {
			if len(args) != 1 || args[0] != "'UTC'" {
				return nil, fmt.Errorf("timestamp must use UTC")
			}
		} else {
			if len(args) != 2 || args[1] != "'UTC'" {
				return nil, fmt.Errorf("timestamp must use UTC")
			}
			n, e := strconv.Atoi(args[0])
			if e != nil || (n != 0 && n != 3 && n != 6) {
				return nil, fmt.Errorf("unsupported timestamp precision")
			}
			t.Scale = n
		}
	case "Array", "Nullable", "LowCardinality":
		if len(args) != 1 {
			return nil, fmt.Errorf("wrapper needs one type")
		}
		if e := child(args[0]); e != nil {
			return nil, e
		}
		if t.Name == "LowCardinality" && t.Children[0].Name != "String" {
			return nil, fmt.Errorf("unsupported low cardinality type")
		}
	case "Tuple":
		seen := map[string]bool{}
		for _, a := range args {
			i := strings.IndexByte(a, ' ')
			if i < 1 {
				return nil, fmt.Errorf("tuple requires named fields")
			}
			name := a[:i]
			if !regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`).MatchString(name) || seen[name] {
				return nil, fmt.Errorf("invalid tuple field")
			}
			seen[name] = true
			t.Fields = append(t.Fields, name)
			if e := child(strings.TrimSpace(a[i:])); e != nil {
				return nil, e
			}
		}
	case "Enum8":
		t.Enums = map[string]int8{}
		seen := map[int8]bool{}
		for _, a := range args {
			m := enumPattern.FindStringSubmatch(a)
			if m == nil {
				return nil, fmt.Errorf("invalid enum")
			}
			v, e := strconv.ParseInt(m[2], 10, 8)
			if e != nil || seen[int8(v)] {
				return nil, fmt.Errorf("invalid enum code")
			}
			if _, ok := t.Enums[m[1]]; ok {
				return nil, fmt.Errorf("duplicate enum name")
			}
			t.Enums[m[1]] = int8(v)
			seen[int8(v)] = true
		}
	default:
		return nil, fmt.Errorf("unsupported type %q", text)
	}
	return t, nil
}
