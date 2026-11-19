package recordings

import (
	"bufio"
	"encoding/json"
	"io"
	"unicode/utf8"
)

func derivedJSONByte(reader *bufio.Reader) (byte, error) {
	for n := 0; n < 4096; n++ {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		if b != ' ' && b != '\n' && b != '\r' && b != '\t' {
			return b, nil
		}
	}
	return 0, ErrDerivedTooLarge
}

// A value is delimited and bounded BEFORE json.Unmarshal can allocate it.
// The fixed reader buffer may prefetch at most one additional KiB.
func derivedJSONValue(reader *bufio.Reader, limit int) (json.RawMessage, error) {
	first, err := derivedJSONByte(reader)
	if err != nil {
		return nil, err
	}
	raw := make([]byte, 0, min(limit, 256))
	raw = append(raw, first)
	depth := 0
	quoted := first == '"'
	inString := quoted
	escaped := false
	if first == '{' || first == '[' {
		depth = 1
	}
	for {
		if len(raw) > limit {
			return nil, ErrDerivedTooLarge
		}
		b, e := reader.ReadByte()
		if e == io.EOF && depth == 0 && !inString {
			break
		}
		if e != nil {
			return nil, ErrDerivedIncompatible
		}
		if depth == 0 && !quoted && (b == ',' || b == ']' || b == '}') {
			if reader.UnreadByte() != nil {
				return nil, ErrDerivedIncompatible
			}
			break
		}
		if depth == 0 && !quoted && (b == ' ' || b == '\n' || b == '\r' || b == '\t') {
			break
		}
		raw = append(raw, b)
		if len(raw) > limit {
			return nil, ErrDerivedTooLarge
		}
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if b == '\\' {
				escaped = true
				continue
			}
			if b == '"' {
				inString = false
				if quoted && depth == 0 {
					break
				}
			}
			continue
		}
		if b == '"' {
			inString = true
			continue
		}
		if b == '{' || b == '[' {
			depth++
			if depth > 32 {
				return nil, ErrDerivedTooLarge
			}
		}
		if b == '}' || b == ']' {
			depth--
			if depth == 0 {
				break
			}
			if depth < 0 {
				return nil, ErrDerivedIncompatible
			}
		}
	}
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return nil, ErrDerivedIncompatible
	}
	return raw, nil
}

func readDerivedArray(input io.Reader, key string, consume func(json.RawMessage) error) error {
	reader := bufio.NewReaderSize(input, 1024)
	b, err := derivedJSONByte(reader)
	if err != nil || b != '{' {
		return ErrDerivedIncompatible
	}
	found, count := false, 0
	keys := map[string]bool{}
	for {
		b, err = derivedJSONByte(reader)
		if err != nil {
			return ErrDerivedIncompatible
		}
		if b == '}' {
			break
		}
		if b != '"' || reader.UnreadByte() != nil {
			return ErrDerivedIncompatible
		}
		raw, err := derivedJSONValue(reader, 128)
		if err != nil {
			return err
		}
		var name string
		if json.Unmarshal(raw, &name) != nil || keys[name] || len(keys) >= 16 {
			return ErrDerivedIncompatible
		}
		keys[name] = true
		b, err = derivedJSONByte(reader)
		if err != nil || b != ':' {
			return ErrDerivedIncompatible
		}
		if name == key {
			if found {
				return ErrDerivedIncompatible
			}
			found = true
			b, err = derivedJSONByte(reader)
			if err != nil || b != '[' {
				return ErrDerivedIncompatible
			}
			for {
				b, err = derivedJSONByte(reader)
				if err != nil {
					return ErrDerivedIncompatible
				}
				if b == ']' {
					break
				}
				if reader.UnreadByte() != nil {
					return ErrDerivedIncompatible
				}
				value, err := derivedJSONValue(reader, 4096)
				if err != nil {
					return err
				}
				count++
				if count > 2_000_000 {
					return ErrDerivedTooLarge
				}
				if err = consume(value); err != nil {
					return err
				}
				b, err = derivedJSONByte(reader)
				if err != nil {
					return ErrDerivedIncompatible
				}
				if b == ']' {
					break
				}
				if b != ',' {
					return ErrDerivedIncompatible
				}
				next, e := derivedJSONByte(reader)
				if e != nil || next == ']' {
					return ErrDerivedIncompatible
				}
				if reader.UnreadByte() != nil {
					return ErrDerivedIncompatible
				}
			}
		} else {
			if _, err := derivedJSONValue(reader, 64<<10); err != nil {
				return err
			}
		}
		b, err = derivedJSONByte(reader)
		if err != nil {
			return ErrDerivedIncompatible
		}
		if b == '}' {
			break
		}
		if b != ',' {
			return ErrDerivedIncompatible
		}
		next, e := derivedJSONByte(reader)
		if e != nil || next == '}' {
			return ErrDerivedIncompatible
		}
		if reader.UnreadByte() != nil {
			return ErrDerivedIncompatible
		}
	}
	if !found || count == 0 {
		return ErrDerivedIncompatible
	}
	if _, err = derivedJSONByte(reader); err != io.EOF {
		return ErrDerivedIncompatible
	}
	return nil
}
