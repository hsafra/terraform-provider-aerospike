// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

// writeTestCACert writes a freshly generated self-signed CA certificate as PEM
// and returns the file path.
func writeTestCACert(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeTestFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "file.pem")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadRootCAs(t *testing.T) {
	t.Run("valid PEM", func(t *testing.T) {
		pool, err := loadRootCAs(writeTestCACert(t))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pool == nil {
			t.Fatal("expected a cert pool")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		if _, err := loadRootCAs(filepath.Join(t.TempDir(), "missing.pem")); err == nil {
			t.Fatal("expected error for missing file")
		}
	})

	t.Run("not PEM", func(t *testing.T) {
		_, err := loadRootCAs(writeTestFile(t, "this is not a certificate"))
		if err == nil {
			t.Fatal("expected error for non-PEM content")
		}
		if !strings.Contains(err.Error(), "no valid PEM certificates") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("empty file", func(t *testing.T) {
		if _, err := loadRootCAs(writeTestFile(t, "")); err == nil {
			t.Fatal("expected error for empty file")
		}
	})
}

// testAccProviderConfigWithRole is a provider block plus a resource, so that
// planning it configures the provider.
func testAccProviderConfigWithRole(providerBody string) string {
	return fmt.Sprintf(`
provider "aerospike" {
%s
}

resource "aerospike_role" "provider_cfg" {
  role_name  = "testrole_provider_cfg"
  privileges = [{privilege="read"}]
}`, providerBody)
}

// An invalid root CA file is reported as a diagnostic, not a panic.
func TestAccProvider_invalidRootCAFile(t *testing.T) {
	path := writeTestFile(t, "this is not a certificate")
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccProviderConfigWithRole(fmt.Sprintf("  tls = { root_ca_file = %q }", path)),
				ExpectError: regexp.MustCompile(`Error reading root ca file`),
			},
		},
	})
}

// A missing root CA file is reported as a diagnostic.
func TestAccProvider_missingRootCAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.pem")
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccProviderConfigWithRole(fmt.Sprintf("  tls = { root_ca_file = %q }", path)),
				ExpectError: regexp.MustCompile(`Error reading root ca file`),
			},
		},
	})
}

// Wrong credentials are reported as a diagnostic, not a panic.
func TestAccProvider_wrongPassword(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			// Environment variables override provider attributes, so set the
			// bad password there, after the precheck has connected.
			t.Setenv("AEROSPIKE_PASSWORD", "definitely-wrong-password")
		},
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccProviderConfigWithRole(""),
				ExpectError: regexp.MustCompile(`(?i)connecting to Aerospike`),
			},
		},
	})
}

// An unreachable host is reported as a diagnostic, not a panic.
func TestAccProvider_unreachableHost(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			t.Setenv("AEROSPIKE_HOST", "127.0.0.1")
			t.Setenv("AEROSPIKE_PORT", "1")
			t.Setenv("AEROSPIKE_CONNECT_TIMEOUT", "2")
		},
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccProviderConfigWithRole(""),
				ExpectError: regexp.MustCompile(`(?i)connecting to Aerospike`),
			},
		},
	})
}

// The port validator rejects values outside 0-65535 at plan time.
func TestAccProvider_portOutOfRange(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config:      testAccProviderConfigWithRole("  port = 70000"),
				ExpectError: regexp.MustCompile(`(?i)port.*between|value must be between`),
			},
		},
	})
}
