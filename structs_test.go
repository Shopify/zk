package zk

import (
	"errors"
	"reflect"
	"testing"
)

func TestEncodeDecodePacket(t *testing.T) {
	t.Parallel()
	encodeDecodeTest(t, &requestHeader{-2, 5})
	encodeDecodeTest(t, &connectResponse{1, 2, 3, nil})
	encodeDecodeTest(t, &connectResponse{1, 2, 3, []byte{4, 5, 6}})
	encodeDecodeTest(t, &getAclResponse{[]ACL{{12, "s", "anyone"}}, Stat{}})
	encodeDecodeTest(t, &getChildrenResponse{[]string{"foo", "bar"}})
	encodeDecodeTest(t, &pathWatchRequest{"path", true})
	encodeDecodeTest(t, &pathWatchRequest{"path", false})
	encodeDecodeTest(t, &CheckVersionRequest{"/", -1})
	encodeDecodeTest(t, &reconfigRequest{nil, nil, nil, -1})
	encodeDecodeTest(t, &multiRequest{Ops: []multiRequestOp{{multiHeader{opCheck, false, -1}, &CheckVersionRequest{"/", -1}}}})
}

func TestRequestStructForOp(t *testing.T) {
	for op, name := range opNames {
		switch op {
		case opNotify, opPing, opWatcherEvent, opClose:
			if s := requestStructForOp(op); s != nil {
				t.Errorf("Expected no struct for op %s, but got one", name)
			}
		default:
			if s := requestStructForOp(op); s == nil {
				t.Errorf("Expected struct for op %s, but got none", name)
			}
		}
	}
}

func encodeDecodeTest(t *testing.T, r any) {
	buf := make([]byte, 1024)
	n, err := encodePacket(buf, r)
	if err != nil {
		t.Errorf("encodePacket returned non-nil error %+v\n", err)
		return
	}
	//t.Logf("%+v %x", r, buf[:n])
	r2 := reflect.New(reflect.ValueOf(r).Elem().Type()).Interface()
	n2, err := decodePacket(buf[:n], r2)
	if err != nil {
		t.Errorf("decodePacket returned non-nil error %+v\n", err)
		return
	}
	if n != n2 {
		t.Errorf("sizes don't match: %d != %d", n, n2)
		return
	}
	if !reflect.DeepEqual(r, r2) {
		t.Errorf("results don't match: %+v != %+v", r, r2)
		return
	}
}

func TestEncodeShortBuffer(t *testing.T) {
	t.Parallel()
	_, err := encodePacket([]byte{}, &requestHeader{1, 2})
	if !errors.Is(err, ErrShortBuffer) {
		t.Errorf("encodePacket should return ErrShortBuffer on a short buffer instead of '%+v'", err)
		return
	}
}

func TestDecodeShortBuffer(t *testing.T) {
	t.Parallel()
	_, err := decodePacket([]byte{}, &responseHeader{})
	if !errors.Is(err, ErrShortBuffer) {
		t.Errorf("decodePacket should return ErrShortBuffer on a short buffer instead of '%+v'", err)
		return
	}
}

func BenchmarkEncode(b *testing.B) {
	buf := make([]byte, 4096)
	st := &connectRequest{Passwd: []byte("1234567890")}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := encodePacket(buf, st); err != nil {
			b.Fatal(err)
		}
	}
}
