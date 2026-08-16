package valkyr

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func uintPtr(value uint64) *uint64 { return &value }

func sameJSON(t *testing.T, want, got []byte) {
	t.Helper()
	var a, b any
	if e := json.Unmarshal(want, &a); e != nil {
		t.Fatal(e)
	}
	if e := json.Unmarshal(got, &b); e != nil {
		t.Fatal(e)
	}
	wa, _ := json.Marshal(a)
	wb, _ := json.Marshal(b)
	if string(wa) != string(wb) {
		t.Fatalf("wire mismatch: %s != %s", wa, wb)
	}
}
func fixture(t *testing.T, name string) []string {
	t.Helper()
	b, e := os.ReadFile("../docs/protocol/fixtures/" + name)
	if e != nil {
		t.Fatal(e)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}
func TestCanonicalWireFixtures(t *testing.T) {
	for _, line := range fixture(t, "commands.txt") {
		c, e := parseTextCommand([]byte(line))
		if e != nil {
			t.Fatal(e, line)
		}
		got, e := textCommand(c)
		if e != nil || string(got) != line {
			t.Fatalf("wire mismatch: %v %s", e, got)
		}
	}
	command := wireCommand{Type: "get", Namespace: "/users", Key: "42"}
	for _, line := range fixture(t, "responses.txt") {
		context := command
		switch line {
		case "OK client-1 TTL 3600", "MISS 10", `KO "invalid API key"`:
			context = wireCommand{Type: "auth"}
		case "PONG":
			context = wireCommand{Type: "ping"}
		case "STATS REQUESTS 10 HITS 5 MISSES 3 VALUES 2":
			context = wireCommand{Type: "stats"}
		case "OK", `KO "provider unavailable"`:
			context = wireCommand{Type: "set"}
		}
		response, e := parseTextResponse([]byte(line), context)
		if e != nil {
			t.Fatal(e, line)
		}
		got, e := textResponse(context, response)
		if e != nil || string(got) != line {
			t.Fatalf("wire mismatch: %v %s", e, got)
		}
	}
	for _, line := range fixture(t, "server_commands.txt") {
		c, e := parseTextServerCommand([]byte(line))
		if e != nil {
			t.Fatal(e, line)
		}
		got, e := textServerCommand(c)
		if e != nil || string(got) != line {
			t.Fatalf("wire mismatch: %v %s", e, got)
		}
	}
	for _, line := range fixture(t, "server_results.txt") {
		c := wireServerCommand{Type: "query", RequestID: "00000000-0000-0000-0000-000000000001", Namespace: "/users", Key: "42"}
		if strings.HasPrefix(line, "OPERATION") {
			c.Type = "persist_set"
			c.RequestID = "00000000-0000-0000-0000-000000000002"
		}
		decoded, e := parseTextServerResult([]byte(line), c)
		if e != nil {
			t.Fatal(e, line)
		}
		got, e := textServerResult(c, decoded)
		if e != nil || string(got) != line {
			t.Fatalf("wire mismatch: %v %s", e, got)
		}
	}
}
func TestWireRejectsUnknownAndMalformedValues(t *testing.T) {
	cases := []struct {
		name  string
		input string
		parse func([]byte) error
	}{
		{"unknown command", `FUTURE`, func(b []byte) error { _, err := parseTextCommand(b); return err }},
		{"invalid callback UUID", `QUERY not-a-uuid /x k`, func(b []byte) error { _, err := parseTextServerCommand(b); return err }},
		{"negative integer", `SET /x k 1 EX -1`, func(b []byte) error { _, err := parseTextCommand(b); return err }},
		{"bare string value", `SET /x k bare-word`, func(b []byte) error { _, err := parseTextCommand(b); return err }},
		{"non-finite value", `SET /x k NaN`, func(b []byte) error { _, err := parseTextCommand(b); return err }},
		{"invalid UTF-8", "SET /x k \xff", func(b []byte) error { _, err := parseTextCommand(b); return err }},
		{"json response rejected", `{"type":"ok"}`, func(b []byte) error { _, err := parseTextResponse(b, wireCommand{Type: "set"}); return err }},
		{"invalid callback result", `FUTURE 00000000-0000-0000-0000-000000000001 OK`, func(b []byte) error {
			_, err := parseTextServerResult(b, wireServerCommand{Type: "query", RequestID: "00000000-0000-0000-0000-000000000001", Namespace: "/x", Key: "k"})
			return err
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := test.parse([]byte(test.input)); err == nil {
				t.Fatal("malformed frame accepted")
			}
		})
	}
}

func TestWireAnswerContextsAndQuotedEXBatches(t *testing.T) {
	get := wireCommand{Type: "get", Namespace: "/x", Key: "k"}
	set := wireCommand{Type: "set", Namespace: "/x", Key: "k", Value: json.RawMessage(`1`)}
	for _, test := range []struct {
		name string
		line []byte
		cmd  wireCommand
	}{
		{"GET rejects OK", []byte("OK"), get},
		{"SET rejects PONG", []byte("PONG"), set},
		{"PING rejects UNKNOWN", []byte("UNKNOWN"), wireCommand{Type: "ping"}},
		{"STATS rejects OK", []byte("OK"), wireCommand{Type: "stats"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseTextResponse(test.line, test.cmd); err == nil {
				t.Fatal("invalid answer context accepted")
			}
		})
	}

	batch := wireCommand{Type: "set_batch", Namespace: "/x", Entries: []wireEntry{{Key: "EX", Value: json.RawMessage(`1`)}}, TTLSec: uintPtr(5)}
	line, err := textCommand(batch)
	if err != nil || string(line) != `SET_BATCH /x "EX" 1 EX 5` {
		t.Fatalf("quoted EX batch formatting failed: %s %v", line, err)
	}
	decoded, err := parseTextCommand(line)
	if err != nil || len(decoded.Entries) != 1 || decoded.Entries[0].Key != "EX" || decoded.TTLSec == nil || *decoded.TTLSec != 5 {
		t.Fatalf("quoted EX batch parsing failed: %#v %v", decoded, err)
	}
	if _, err = parseTextCommand([]byte(`SET_BATCH /x EX 5`)); err == nil {
		t.Fatal("empty batch accepted")
	}
	callback := wireServerCommand{Type: "persist_set_batch", RequestID: "00000000-0000-0000-0000-000000000003", Namespace: "/x", Entries: []wireEntry{{Key: "EX", Value: json.RawMessage(`1`)}}}
	callbackLine, err := textServerCommand(callback)
	if err != nil || string(callbackLine) != `PERSIST_SET_BATCH 00000000-0000-0000-0000-000000000003 /x "EX" 1` {
		t.Fatalf("quoted callback batch formatting failed: %s %v", callbackLine, err)
	}
	if _, err = parseTextServerCommand(callbackLine); err != nil {
		t.Fatalf("quoted callback batch rejected: %v", err)
	}
}

func TestWireRejectsOverflowingValuesAndQuotedKeywords(t *testing.T) {
	for _, line := range []string{
		"SET /x k 1e999",
		"SET /x k -1e999",
		`SET /x k {"nested":[1e999]}`,
		`AUTH key "ADAPTER" 00000000-0000-0000-0000-000000000001`,
		`SET /x k 1 "EX" 5`,
		`PROVIDE /x * "MAX_RATE" 1`,
		`PROVIDE /x * TIMEOUT 1 "MISS_TTL" 2`,
	} {
		if _, err := parseTextCommand([]byte(line)); err == nil {
			t.Fatalf("accepted invalid command: %s", line)
		}
	}

	finite, err := parseTextCommand([]byte("SET /x k 1e100"))
	if err != nil || string(finite.Value) != "1e100" {
		t.Fatalf("finite scalar exponent was not preserved: %#v %v", finite, err)
	}
	nested, err := parseTextCommand([]byte(`SET /x k {"nested":-1e-100}`))
	if err != nil || string(nested.Value) != `{"nested":-1e-100}` {
		t.Fatalf("finite nested exponent was not preserved: %#v %v", nested, err)
	}
	for _, value := range []json.RawMessage{json.RawMessage(`1e999`), json.RawMessage(`{"nested":-1e999}`)} {
		if _, err := textCommand(wireCommand{Type: "set", Namespace: "/x", Key: "k", Value: value}); err == nil {
			t.Fatalf("formatted overflowing value: %s", value)
		}
	}

	get := wireCommand{Type: "get", Namespace: "/x", Key: "k"}
	for _, line := range []string{
		`OK client "TTL" 1`,
		`SET /x k 1 "EX" 5`,
		`STATS "REQUESTS" 1 HITS 0 MISSES 0 VALUES 0`,
	} {
		context := get
		if strings.HasPrefix(line, "OK") {
			context = wireCommand{Type: "auth"}
		} else if strings.HasPrefix(line, "STATS") {
			context = wireCommand{Type: "stats"}
		}
		if _, err := parseTextResponse([]byte(line), context); err == nil {
			t.Fatalf("accepted invalid answer: %s", line)
		}
	}

	if _, err := parseTextServerCommand([]byte(
		`PERSIST_SET 00000000-0000-0000-0000-000000000001 /x k 1 "EX" 5`,
	)); err == nil {
		t.Fatal("accepted quoted callback option")
	}
	query := wireServerCommand{Type: "query", RequestID: "00000000-0000-0000-0000-000000000001", Namespace: "/x", Key: "k"}
	if _, err := parseTextServerResult([]byte(
		`QUERY_RESULT 00000000-0000-0000-0000-000000000001 "SET" /x k 1`,
	), query); err == nil {
		t.Fatal("accepted quoted query result marker")
	}
	persist := wireServerCommand{Type: "persist_set", RequestID: "00000000-0000-0000-0000-000000000002", Namespace: "/x", Key: "k", Value: json.RawMessage(`1`)}
	if _, err := parseTextServerResult([]byte(
		`OPERATION 00000000-0000-0000-0000-000000000002 "OK"`,
	), persist); err == nil {
		t.Fatal("accepted quoted operation marker")
	}
}

func TestProviderPolicyFieldsRoundTripAndRejectInvalidValues(t *testing.T) {
	c, err := parseTextCommand([]byte(`PROVIDE /values * TIMEOUT 250 MISS_TTL 30`))
	if err != nil || c.Timeout == nil || *c.Timeout != 250 || c.MissTTL == nil || *c.MissTTL != 30 {
		t.Fatalf("provider policy did not decode: %#v %v", c, err)
	}
	if _, err = parseTextCommand([]byte(`PROVIDE /values * MISS_TTL 30 TIMEOUT 250`)); err == nil {
		t.Fatal("out-of-order provider options accepted")
	}
}
func TestDurationConversions(t *testing.T) {
	d := 5 * time.Second
	n, e := durationSeconds(&d)
	if e != nil || *n != 5 {
		t.Fatalf("duration conversion failed: %v", e)
	}
	bad := time.Millisecond
	if _, e = durationSeconds(&bad); e == nil {
		t.Fatal("fractional duration accepted")
	}
	negative := -time.Second
	if _, e = durationSeconds(&negative); e == nil {
		t.Fatal("negative duration accepted")
	}
}
