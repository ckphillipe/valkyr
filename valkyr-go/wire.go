package valkyr

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const maxUint64 = ^uint64(0)

type wireCommand struct {
	Type             string
	APIKey           string
	AdapterInstance  *string
	Namespace        string
	Key              string
	Value            json.RawMessage
	TTLSec           *uint64
	Entries          []wireEntry
	KeyPattern       *string
	Source           string
	Destination      string
	NamespacePattern string
	MaxRate          *uint64
	Timeout          *uint64
	MissTTL          *uint64
}
type wireEntry struct {
	Key   string
	Value json.RawMessage
}
type wireResponse struct {
	Type       string
	Value      json.RawMessage
	TTL        *uint64
	RetryAfter *uint64
	ClientID   string
	SessionTTL *uint64
	Message    string
	Requests   *uint64
	Hits       *uint64
	Misses     *uint64
	Values     *uint64
}
type wireServerCommand struct {
	Type        string
	RequestID   string
	Namespace   string
	Key         string
	Value       json.RawMessage
	TTL         *uint64
	Entries     []wireEntry
	KeyPattern  *string
	Source      string
	Destination string
}
type wireServerResult struct {
	Type      string
	RequestID string
	Value     json.RawMessage
	Error     *string
	TTL       *uint64
}

func protocol(msg string) error { return fmt.Errorf("%w: %s", ErrProtocol, msg) }

func uuidValue(s string, required bool) (string, error) {
	if s == "" && !required {
		return "", nil
	}
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return "", protocol("invalid canonical UUID")
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return "", protocol("invalid canonical UUID")
		}
	}
	return s, nil
}
func durationSeconds(d *time.Duration) (*uint64, error) {
	if d == nil {
		return nil, nil
	}
	if *d < 0 || *d%time.Second != 0 {
		return nil, protocol("duration must be a non-negative whole number of seconds")
	}
	n := uint64(*d / time.Second)
	return &n, nil
}
func durationMillis(ms uint64) time.Duration {
	if ms > uint64(time.Duration(1<<63-1))/uint64(time.Millisecond) {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(ms) * time.Millisecond
}
func protocolDurationMillis(value time.Duration) (*uint64, error) {
	if value < 0 || value%time.Millisecond != 0 {
		return nil, protocol("provider timeout must be a non-negative whole number of milliseconds")
	}
	n := uint64(value / time.Millisecond)
	return &n, nil
}
func protocolDurationSeconds(value time.Duration) (*uint64, error) {
	if value < 0 || value%time.Second != 0 {
		return nil, protocol("provider miss TTL must be a non-negative whole number of seconds")
	}
	n := uint64(value / time.Second)
	return &n, nil
}

func quoteText(value string) string {
	if value != "" && strings.IndexFunc(value, func(r rune) bool {
		return !(r == '_' || r == '.' || r == '/' || r == ':' || r == '*' || r == '~' || r == '$' || r == '-' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9')
	}) == -1 {
		return value
	}
	b, _ := json.Marshal(value)
	return string(b)
}

type textToken struct {
	value  string
	quoted bool
}

func (t textToken) text() string { return t.value }

func isUnquoted(tokens []textToken, index int, expected string) bool {
	return index < len(tokens) && !tokens[index].quoted && tokens[index].text() == expected
}

func batchKeyText(value string) string {
	if value == "EX" {
		b, _ := json.Marshal(value)
		return string(b)
	}
	return quoteText(value)
}

func textTokens(data []byte) ([]textToken, error) {
	if !utf8.Valid(data) {
		return nil, protocol("frame is not valid UTF-8")
	}
	s := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	if s == "" || len(data) > maxFrameBytes {
		return nil, protocol("empty or oversized frame")
	}
	var out []textToken
	for i := 0; i < len(s); {
		for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
		if i == len(s) {
			break
		}
		start := i
		if s[i] == '"' {
			end, escaped := i+1, false
			for end < len(s) {
				if escaped {
					escaped = false
				} else if s[end] == '\\' {
					escaped = true
				} else if s[end] == '"' {
					end++
					break
				}
				end++
			}
			if end > len(s) || s[end-1] != '"' {
				return nil, protocol("unterminated quoted token")
			}
			var value string
			if json.Unmarshal([]byte(s[start:end]), &value) != nil {
				return nil, protocol("invalid quoted token")
			}
			out = append(out, textToken{value: value, quoted: true})
			i = end
			continue
		}
		if s[i] == '[' || s[i] == '{' {
			stack := []byte{s[i]}
			i++
			quoted, escaped := false, false
			for i < len(s) {
				ch := s[i]
				i++
				if quoted {
					if escaped {
						escaped = false
					} else if ch == '\\' {
						escaped = true
					} else if ch == '"' {
						quoted = false
					}
					continue
				}
				if ch == '"' {
					quoted = true
				} else if ch == '[' || ch == '{' {
					stack = append(stack, ch)
				} else if ch == ']' || ch == '}' {
					expected := byte('[')
					if ch == '}' {
						expected = '{'
					}
					if len(stack) == 0 || stack[len(stack)-1] != expected {
						return nil, protocol("unbalanced structured value")
					}
					stack = stack[:len(stack)-1]
					if len(stack) == 0 {
						raw := s[start:i]
						var value any
						if json.Unmarshal([]byte(raw), &value) != nil {
							return nil, protocol("invalid structured value")
						}
						out = append(out, textToken{value: raw})
						break
					}
				}
			}
			if len(stack) != 0 {
				return nil, protocol("incomplete structured value")
			}
			continue
		}
		for i < len(s) && s[i] != ' ' && s[i] != '\t' {
			i++
		}
		out = append(out, textToken{value: s[start:i]})
	}
	if len(out) == 0 {
		return nil, protocol("empty frame")
	}
	return out, nil
}
func u64Text(value string) (uint64, error) {
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, protocol("invalid unsigned integer")
	}
	return n, nil
}
func numText(s string) (*uint64, error) { n, e := u64Text(s); return &n, e }
func rawTextValue(token textToken) ([]byte, error) {
	if token.quoted {
		return json.Marshal(token.value)
	}
	value := []byte(token.value)
	if err := validateJSONValue(value); err != nil {
		return nil, err
	}
	return value, nil
}

func strictValue(value []byte) (string, error) {
	if err := validateJSONValue(value); err != nil {
		return "", err
	}
	return string(value), nil
}

func validateJSONValue(value []byte) error {
	if !json.Valid(value) {
		return protocol("invalid value literal")
	}
	decoder := json.NewDecoder(strings.NewReader(string(value)))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return protocol("invalid value literal")
	}
	if err := validateJSONNumbers(decoded); err != nil {
		return err
	}
	return nil
}

func validateJSONNumbers(value any) error {
	switch value := value.(type) {
	case json.Number:
		if parsed, err := strconv.ParseFloat(value.String(), 64); err != nil || math.IsInf(parsed, 0) {
			return protocol("numeric value overflows float64")
		}
	case []any:
		for _, item := range value {
			if err := validateJSONNumbers(item); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, item := range value {
			if err := validateJSONNumbers(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func textCommand(c wireCommand) ([]byte, error) {
	var s string
	switch c.Type {
	case "auth":
		s = "AUTH " + quoteText(c.APIKey)
		if c.AdapterInstance != nil {
			if _, err := uuidValue(*c.AdapterInstance, true); err != nil {
				return nil, err
			}
			s += " ADAPTER " + *c.AdapterInstance
		}
	case "get":
		s = "GET " + quoteText(c.Namespace) + " " + quoteText(c.Key)
	case "set":
		value, err := strictValue(c.Value)
		if err != nil {
			return nil, err
		}
		s = "SET " + quoteText(c.Namespace) + " " + quoteText(c.Key) + " " + value
		if c.TTLSec != nil {
			s += " EX " + strconv.FormatUint(*c.TTLSec, 10)
		}
	case "set_batch":
		if len(c.Entries) == 0 {
			return nil, protocol("SET_BATCH requires an entry")
		}
		s = "SET_BATCH " + quoteText(c.Namespace)
		for _, e := range c.Entries {
			value, err := strictValue(e.Value)
			if err != nil {
				return nil, err
			}
			s += " " + batchKeyText(e.Key) + " " + value
		}
		if c.TTLSec != nil {
			s += " EX " + strconv.FormatUint(*c.TTLSec, 10)
		}
	case "delete":
		s = "DELETE " + quoteText(c.Namespace)
		if c.KeyPattern != nil {
			s += " " + quoteText(*c.KeyPattern)
		}
	case "move":
		s = "MOVE " + quoteText(c.Source) + " " + quoteText(c.Destination)
	case "provide":
		if c.KeyPattern == nil {
			return nil, protocol("PROVIDE requires a key pattern")
		}
		s = "PROVIDE " + quoteText(c.NamespacePattern) + " " + quoteText(*c.KeyPattern)
		for _, o := range []struct {
			name  string
			value *uint64
		}{{"MAX_RATE", c.MaxRate}, {"TIMEOUT", c.Timeout}, {"MISS_TTL", c.MissTTL}} {
			if o.value != nil {
				s += " " + o.name + " " + strconv.FormatUint(*o.value, 10)
			}
		}
	case "store":
		if c.KeyPattern == nil {
			return nil, protocol("STORE requires a key pattern")
		}
		s = "STORE " + quoteText(c.NamespacePattern) + " " + quoteText(*c.KeyPattern)
	case "ping":
		s = "PING"
	case "stats":
		s = "STATS"
	default:
		return nil, protocol("unknown command")
	}
	return []byte(s), nil
}

func parseTextCommand(data []byte) (wireCommand, error) {
	t, err := textTokens(data)
	if err != nil {
		return wireCommand{}, err
	}
	need := func(n int) error {
		if len(t) != n {
			return protocol("unexpected number of arguments")
		}
		return nil
	}
	num := func(i int) (*uint64, error) { n, e := u64Text(t[i].text()); return &n, e }
	c := wireCommand{}
	if t[0].quoted {
		return c, protocol("command keyword must be unquoted")
	}
	switch t[0].text() {
	case "AUTH":
		if len(t) != 2 && (len(t) != 4 || !isUnquoted(t, 2, "ADAPTER")) {
			return c, protocol("invalid AUTH")
		}
		c = wireCommand{Type: "auth", APIKey: t[1].text()}
		if len(t) == 4 {
			id, e := uuidValue(t[3].text(), true)
			if e != nil {
				return c, e
			}
			c.AdapterInstance = &id
		}
	case "GET":
		if err = need(3); err == nil {
			c = wireCommand{Type: "get", Namespace: t[1].text(), Key: t[2].text()}
		}
	case "SET":
		if len(t) != 4 && len(t) != 6 {
			err = protocol("invalid SET")
		} else {
			value, valueErr := rawTextValue(t[3])
			if valueErr != nil {
				err = valueErr
				break
			}
			c = wireCommand{Type: "set", Namespace: t[1].text(), Key: t[2].text(), Value: value}
			if len(t) == 6 {
				if t[4].quoted || t[4].text() != "EX" {
					err = protocol("invalid SET option")
				} else {
					c.TTLSec, err = num(5)
				}
			}
		}
	case "SET_BATCH":
		if len(t) < 4 {
			err = protocol("invalid SET_BATCH")
		} else {
			end := len(t)
			if len(t) >= 4 && !t[len(t)-2].quoted && t[len(t)-2].text() == "EX" {
				end -= 2
				c.TTLSec, err = num(len(t) - 1)
			}
			if err == nil && (end-2)%2 != 0 {
				err = protocol("invalid batch")
			}
			c.Type, c.Namespace = "set_batch", t[1].text()
			for i := 2; err == nil && i < end; i += 2 {
				value, valueErr := rawTextValue(t[i+1])
				if valueErr != nil {
					err = valueErr
					break
				}
				c.Entries = append(c.Entries, wireEntry{Key: t[i].text(), Value: value})
			}
			if err == nil && len(c.Entries) == 0 {
				err = protocol("SET_BATCH requires an entry")
			}
		}
	case "DELETE":
		if len(t) != 2 && len(t) != 3 {
			err = protocol("invalid DELETE")
		} else {
			c.Type, c.Namespace = "delete", t[1].text()
			if len(t) == 3 {
				value := t[2].text()
				c.KeyPattern = &value
			}
		}
	case "MOVE":
		if err = need(3); err == nil {
			c = wireCommand{Type: "move", Source: t[1].text(), Destination: t[2].text()}
		}
	case "PROVIDE":
		if len(t) < 3 {
			err = protocol("invalid PROVIDE")
		} else {
			keyPattern := t[2].text()
			c.Type, c.NamespacePattern, c.KeyPattern = "provide", t[1].text(), &keyPattern
			options := map[string]struct {
				index int
				dest  **uint64
			}{
				"MAX_RATE": {0, &c.MaxRate}, "TIMEOUT": {1, &c.Timeout}, "MISS_TTL": {2, &c.MissTTL},
			}
			i := 3
			last := -1
			for i < len(t) {
				o, ok := options[t[i].text()]
				if !ok || t[i].quoted || o.index <= last || i+1 == len(t) {
					err = protocol("invalid PROVIDE option order")
					break
				}
				*o.dest, err = num(i + 1)
				last = o.index
				i += 2
				if err != nil {
					break
				}
			}
			if err == nil && i != len(t) {
				err = protocol("invalid PROVIDE option")
			}
		}
	case "STORE":
		if err = need(3); err == nil {
			keyPattern := t[2].text()
			c = wireCommand{Type: "store", NamespacePattern: t[1].text(), KeyPattern: &keyPattern}
		}
	case "PING":
		if err = need(1); err == nil {
			c.Type = "ping"
		}
	case "STATS":
		if err = need(1); err == nil {
			c.Type = "stats"
		}
	default:
		err = protocol("unknown command")
	}
	return c, err
}

func textResponse(c wireCommand, r wireResponse) ([]byte, error) {
	switch r.Type {
	case "ok":
		if !isMutation(c.Type) {
			return nil, protocol("OK answer has invalid context")
		}
		return []byte("OK"), nil
	case "value":
		if c.Type != "get" || r.Value == nil {
			return nil, protocol("value requires GET")
		}
		value, err := strictValue(r.Value)
		if err != nil {
			return nil, err
		}
		s := "SET " + quoteText(c.Namespace) + " " + quoteText(c.Key) + " " + value
		if r.TTL != nil {
			s += " EX " + strconv.FormatUint(*r.TTL, 10)
		}
		return []byte(s), nil
	case "miss", "auth_pending":
		if (r.Type == "miss" && c.Type != "get") || (r.Type == "auth_pending" && c.Type != "auth") {
			return nil, protocol("MISS answer has invalid context")
		}
		if r.RetryAfter == nil {
			return nil, protocol("MISS requires a retry delay")
		}
		return []byte("MISS " + strconv.FormatUint(*r.RetryAfter, 10)), nil
	case "unknown":
		if c.Type != "get" {
			return nil, protocol("UNKNOWN answer has invalid context")
		}
		return []byte("UNKNOWN"), nil
	case "auth_success":
		if c.Type != "auth" || r.SessionTTL == nil {
			return nil, protocol("authentication success requires AUTH context")
		}
		return []byte("OK " + quoteText(r.ClientID) + " TTL " + strconv.FormatUint(*r.SessionTTL, 10)), nil
	case "auth_failure", "error":
		if (r.Type == "auth_failure" && c.Type != "auth") || (r.Type == "error" && c.Type == "auth") {
			return nil, protocol("KO answer has invalid context")
		}
		return []byte("KO " + quoteText(r.Message)), nil
	case "pong":
		if c.Type != "ping" {
			return nil, protocol("PONG answer has invalid context")
		}
		return []byte("PONG"), nil
	case "stats":
		if c.Type != "stats" {
			return nil, protocol("STATS answer has invalid context")
		}
		if r.Requests == nil || r.Hits == nil || r.Misses == nil || r.Values == nil {
			return nil, protocol("STATS requires all counters")
		}
		return []byte(fmt.Sprintf("STATS REQUESTS %d HITS %d MISSES %d VALUES %d", *r.Requests, *r.Hits, *r.Misses, *r.Values)), nil
	default:
		return nil, protocol("unknown response")
	}
}

func isMutation(commandType string) bool {
	return commandType == "set" || commandType == "set_batch" || commandType == "delete" || commandType == "move" || commandType == "provide" || commandType == "store"
}

func parseTextResponse(data []byte, c wireCommand) (wireResponse, error) {
	t, err := textTokens(data)
	if err != nil {
		return wireResponse{}, err
	}
	r := wireResponse{}
	if t[0].quoted {
		return r, protocol("answer keyword must be unquoted")
	}
	switch t[0].text() {
	case "OK":
		if c.Type == "auth" {
			if len(t) != 4 || !isUnquoted(t, 2, "TTL") {
				return r, protocol("invalid auth answer")
			}
			r.Type, r.ClientID = "auth_success", t[1].text()
			r.SessionTTL, err = numText(t[3].text())
		} else if len(t) == 1 && isMutation(c.Type) {
			r.Type = "ok"
		} else {
			return r, protocol("invalid OK")
		}
	case "SET":
		if c.Type != "get" || (len(t) != 4 && len(t) != 6) || t[1].text() != c.Namespace || t[2].text() != c.Key {
			return r, protocol("SET route mismatch")
		}
		r.Type, r.Value, err = "value", nil, nil
		r.Value, err = rawTextValue(t[3])
		if len(t) == 6 {
			if t[4].quoted || t[4].text() != "EX" {
				return r, protocol("invalid SET option")
			}
			r.TTL, err = numText(t[5].text())
		}
	case "MISS":
		if len(t) != 2 {
			return r, protocol("invalid MISS")
		}
		r.RetryAfter, err = numText(t[1].text())
		if err != nil {
			return r, err
		}
		if c.Type == "auth" {
			r.Type = "auth_pending"
		} else if c.Type == "get" {
			r.Type = "miss"
		} else {
			return r, protocol("MISS answer has invalid context")
		}
	case "UNKNOWN":
		if len(t) != 1 || c.Type != "get" {
			return r, protocol("invalid UNKNOWN")
		}
		r.Type = "unknown"
	case "PONG":
		if len(t) != 1 || c.Type != "ping" {
			return r, protocol("invalid PONG")
		}
		r.Type = "pong"
	case "KO":
		if len(t) != 2 {
			return r, protocol("invalid KO")
		}
		r.Message, r.Type = t[1].text(), "error"
		if c.Type == "auth" {
			r.Type = "auth_failure"
		}
	case "STATS":
		if len(t) != 9 || c.Type != "stats" || !isUnquoted(t, 1, "REQUESTS") || !isUnquoted(t, 3, "HITS") || !isUnquoted(t, 5, "MISSES") || !isUnquoted(t, 7, "VALUES") {
			return r, protocol("invalid STATS")
		}
		r.Type = "stats"
		if r.Requests, err = numText(t[2].text()); err != nil {
			return r, err
		}
		if r.Hits, err = numText(t[4].text()); err != nil {
			return r, err
		}
		if r.Misses, err = numText(t[6].text()); err != nil {
			return r, err
		}
		if r.Values, err = numText(t[8].text()); err != nil {
			return r, err
		}
	default:
		return r, protocol("unknown response")
	}
	return r, err
}

func textServerCommand(c wireServerCommand) ([]byte, error) {
	if _, err := uuidValue(c.RequestID, true); err != nil {
		return nil, err
	}
	switch c.Type {
	case "query":
		return []byte(fmt.Sprintf("QUERY %s %s %s", c.RequestID, quoteText(c.Namespace), quoteText(c.Key))), nil
	case "persist_set":
		value, err := strictValue(c.Value)
		if err != nil {
			return nil, err
		}
		s := fmt.Sprintf("PERSIST_SET %s %s %s %s", c.RequestID, quoteText(c.Namespace), quoteText(c.Key), value)
		if c.TTL != nil {
			s += " EX " + strconv.FormatUint(*c.TTL, 10)
		}
		return []byte(s), nil
	case "persist_set_batch":
		if len(c.Entries) == 0 {
			return nil, protocol("PERSIST_SET_BATCH requires an entry")
		}
		s := fmt.Sprintf("PERSIST_SET_BATCH %s %s", c.RequestID, quoteText(c.Namespace))
		for _, e := range c.Entries {
			value, err := strictValue(e.Value)
			if err != nil {
				return nil, err
			}
			s += " " + batchKeyText(e.Key) + " " + value
		}
		if c.TTL != nil {
			s += " EX " + strconv.FormatUint(*c.TTL, 10)
		}
		return []byte(s), nil
	case "persist_delete":
		s := fmt.Sprintf("PERSIST_DELETE %s %s", c.RequestID, quoteText(c.Namespace))
		if c.KeyPattern != nil {
			s += " " + quoteText(*c.KeyPattern)
		}
		return []byte(s), nil
	case "persist_move":
		return []byte(fmt.Sprintf("PERSIST_MOVE %s %s %s", c.RequestID, quoteText(c.Source), quoteText(c.Destination))), nil
	default:
		return nil, protocol("unknown server command")
	}
}
func parseTextServerCommand(data []byte) (wireServerCommand, error) {
	t, err := textTokens(data)
	if err != nil {
		return wireServerCommand{}, err
	}
	if len(t) < 2 {
		return wireServerCommand{}, protocol("invalid callback")
	}
	id, err := uuidValue(t[1].text(), true)
	if err != nil {
		return wireServerCommand{}, err
	}
	c := wireServerCommand{RequestID: id}
	if t[0].quoted {
		return c, protocol("callback keyword must be unquoted")
	}
	switch t[0].text() {
	case "QUERY":
		if len(t) != 4 {
			return c, protocol("invalid QUERY")
		}
		c.Type, c.Namespace, c.Key = "query", t[2].text(), t[3].text()
	case "PERSIST_SET":
		if len(t) != 5 && len(t) != 7 {
			return c, protocol("invalid PERSIST_SET")
		}
		value, valueErr := rawTextValue(t[4])
		if valueErr != nil {
			return c, valueErr
		}
		c.Type, c.Namespace, c.Key, c.Value = "persist_set", t[2].text(), t[3].text(), value
		if len(t) == 7 {
			if t[5].quoted || t[5].text() != "EX" {
				return c, protocol("invalid PERSIST_SET option")
			}
			c.TTL, err = numText(t[6].text())
		}
	case "PERSIST_SET_BATCH":
		if len(t) < 5 {
			return c, protocol("invalid callback batch")
		}
		c.Type, c.Namespace = "persist_set_batch", t[2].text()
		end := len(t)
		if len(t) >= 5 && !t[len(t)-2].quoted && t[len(t)-2].text() == "EX" {
			end -= 2
			c.TTL, err = numText(t[len(t)-1].text())
		}
		if err == nil && (end-3)%2 != 0 {
			err = protocol("invalid callback batch")
		}
		for i := 3; err == nil && i < end; i += 2 {
			value, valueErr := rawTextValue(t[i+1])
			if valueErr != nil {
				err = valueErr
				break
			}
			c.Entries = append(c.Entries, wireEntry{Key: t[i].text(), Value: value})
		}
		if err == nil && len(c.Entries) == 0 {
			err = protocol("PERSIST_SET_BATCH requires an entry")
		}
	case "PERSIST_DELETE":
		if len(t) != 3 && len(t) != 4 {
			return c, protocol("invalid PERSIST_DELETE")
		}
		c.Type, c.Namespace = "persist_delete", t[2].text()
		if len(t) == 4 {
			value := t[3].text()
			c.KeyPattern = &value
		}
	case "PERSIST_MOVE":
		if len(t) != 4 {
			return c, protocol("invalid PERSIST_MOVE")
		}
		c.Type, c.Source, c.Destination = "persist_move", t[2].text(), t[3].text()
	default:
		return c, protocol("unknown callback")
	}
	return c, err
}
func parseTextServerResult(data []byte, c wireServerCommand) (wireServerResult, error) {
	t, err := textTokens(data)
	if err != nil {
		return wireServerResult{}, err
	}
	if len(t) < 3 {
		return wireServerResult{}, protocol("invalid callback result")
	}
	id, err := uuidValue(t[1].text(), true)
	if err != nil {
		return wireServerResult{}, err
	}
	if id != c.RequestID {
		return wireServerResult{}, protocol("callback correlation mismatch")
	}
	if t[0].quoted {
		return wireServerResult{}, protocol("callback result keyword must be unquoted")
	}
	if c.Type == "query" {
		if !isUnquoted(t, 0, "QUERY_RESULT") {
			return wireServerResult{}, protocol("callback result kind mismatch")
		}
		r := wireServerResult{Type: "query", RequestID: id}
		switch t[2].text() {
		case "MISS":
			if t[2].quoted {
				return r, protocol("invalid query result")
			}
			if len(t) != 3 {
				return r, protocol("invalid query result")
			}
		case "KO":
			if t[2].quoted {
				return r, protocol("invalid query result")
			}
			if len(t) != 4 {
				return r, protocol("invalid query result")
			}
			value := t[3].text()
			r.Error = &value
		case "SET":
			if t[2].quoted {
				return r, protocol("invalid query result")
			}
			if (len(t) != 6 && len(t) != 8) || t[3].text() != c.Namespace || t[4].text() != c.Key {
				return r, protocol("callback route mismatch")
			}
			r.Value, err = rawTextValue(t[5])
			if len(t) == 8 {
				if t[6].quoted || t[6].text() != "EX" {
					return r, protocol("invalid query result option")
				}
				r.TTL, err = numText(t[7].text())
			}
		default:
			return r, protocol("invalid query result")
		}
		return r, err
	}
	if !isUnquoted(t, 0, "OPERATION") {
		return wireServerResult{}, protocol("callback result kind mismatch")
	}
	r := wireServerResult{Type: "operation", RequestID: id}
	if isUnquoted(t, 2, "OK") && len(t) == 3 {
		return r, nil
	}
	if isUnquoted(t, 2, "KO") && len(t) == 4 {
		value := t[3].text()
		r.Error = &value
		return r, nil
	}
	return r, protocol("invalid operation result")
}
func textServerResult(c wireServerCommand, r wireServerResult) ([]byte, error) {
	if r.RequestID != c.RequestID {
		return nil, protocol("callback correlation mismatch")
	}
	if c.Type == "query" {
		if r.Type != "query" {
			return nil, protocol("callback result kind mismatch")
		}
		if r.Error != nil {
			return []byte(fmt.Sprintf("QUERY_RESULT %s KO %s", r.RequestID, quoteText(*r.Error))), nil
		}
		if len(r.Value) == 0 || string(r.Value) == "null" {
			return []byte(fmt.Sprintf("QUERY_RESULT %s MISS", r.RequestID)), nil
		}
		value, err := strictValue(r.Value)
		if err != nil {
			return nil, err
		}
		s := fmt.Sprintf("QUERY_RESULT %s SET %s %s %s", r.RequestID, quoteText(c.Namespace), quoteText(c.Key), value)
		if r.TTL != nil {
			s += " EX " + strconv.FormatUint(*r.TTL, 10)
		}
		return []byte(s), nil
	}
	if r.Type != "operation" {
		return nil, protocol("callback result kind mismatch")
	}
	s := "OPERATION " + r.RequestID + " "
	if r.Error == nil {
		s += "OK"
	} else {
		s += "KO " + quoteText(*r.Error)
	}
	return []byte(s), nil
}
