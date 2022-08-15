package zk

import (
	"testing"
)

func TestRefreshDNSHostProviderReconnection(t *testing.T) {
	hp := refreshDNSHostProvider(t)

	hp.Connected()

	hp.DNSHostProvider.lookupHost = func(host string) ([]string, error) {
		return []string{"10.0.0.1"}, nil
	}

	ip, _ := hp.Next()
	if ip != "10.0.0.1:2181" {
		t.Fatalf("expected ip to be 10.0.0.1, got %v", ip)
	}
}

func TestRefreshDNSHostProviderRetryStart(t *testing.T) {
	hp := refreshDNSHostProvider(t)

	hp.DNSHostProvider.lookupHost = func(host string) ([]string, error) {
		return []string{"10.0.0.1"}, nil
	}

	for {
		if _, retryStart := hp.Next(); retryStart {
			break
		}
	}

	ip, _ := hp.Next()
	if ip != "10.0.0.1:2181" {
		t.Fatalf("expected ip to be 10.0.0.1, got %v", ip)
	}
}

func refreshDNSHostProvider(t *testing.T) *RefreshDNSHostProvider {
	t.Helper()
	hp := &RefreshDNSHostProvider{}

	hp.DNSHostProvider.lookupHost = func(host string) ([]string, error) {
		return []string{"192.0.2.1", "192.0.2.2", "192.0.2.3"}, nil
	}

	if err := hp.Init([]string{"foo.local:2181"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if hp.Len() != 3 {
		t.Fatalf("unexpected length: %d", hp.Len())
	}

	return hp
}
