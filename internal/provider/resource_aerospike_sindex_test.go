// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func testAccSindexPreCheck(t *testing.T) {
	t.Helper()
	if os.Getenv(resource.EnvTfAcc) == "" {
		t.Skipf("acceptance tests skipped unless env %s is set", resource.EnvTfAcc)
	}
	testAccPreCheck(t)

	if !testAccServerSupportsSetSindex(t) {
		t.Skip("aerospike_sindex set indexes require Aerospike Database 8.1.2 or later")
	}

	client, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("Unable to connect to Aerospike: %s", err)
	}
	defer client.Close()

	adminPol := as.NewAdminPolicy()
	_ = client.GrantRoles(adminPol, "admin", []string{"sindex-admin", "sys-admin", "read-write"})
}

func testAccServerSupportsSetSindex(t *testing.T) bool {
	t.Helper()
	client, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("Unable to connect to Aerospike: %s", err)
	}
	defer client.Close()

	ok, err := clusterSupportsSetSindex(client)
	if err != nil {
		t.Fatalf("Unable to read Aerospike version: %s", err)
	}
	return ok
}

func testAccCheckAerospikeSindexDestroy(s *terraform.State) error {
	client, err := testAccGetAerospikeClient()
	if err != nil {
		return err
	}
	defer client.Close()

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aerospike_sindex" {
			continue
		}
		namespace := rs.Primary.Attributes["namespace"]
		name := rs.Primary.Attributes["name"]
		exists, existsErr := sindexExists(client, namespace, name)
		if existsErr != nil {
			return existsErr
		}
		if exists {
			return fmt.Errorf("sindex %q still exists in namespace %q", name, namespace)
		}
	}
	return nil
}

func testAccCheckSetSindexPresent(namespace, setName, name string) resource.TestCheckFunc { //nolint:unparam // namespace will vary as more tests are added
	return func(_ *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		entry, err := getSetIndex(client, namespace, setName)
		if err != nil {
			return err
		}
		if entry == nil {
			return fmt.Errorf("no SMD set index on %s/%s", namespace, setName)
		}
		if name != "" && entry.Name != name {
			return fmt.Errorf("set index on %s/%s: name %q, want %q", namespace, setName, entry.Name, name)
		}
		return nil
	}
}

func testAccCheckSetSindexAbsent(namespace, setName string) resource.TestCheckFunc { //nolint:unparam // namespace will vary as more tests are added
	return func(_ *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		entry, err := getSetIndex(client, namespace, setName)
		if err != nil {
			return err
		}
		if entry != nil {
			return fmt.Errorf("SMD set index still present on %s/%s: %+v", namespace, setName, *entry)
		}
		return nil
	}
}

func testAccCheckSetIndexCount(namespace, setName string, want int) resource.TestCheckFunc { //nolint:unparam // namespace will vary as more tests are added
	return func(_ *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		entries, err := listSindexes(client, namespace)
		if err != nil {
			return err
		}
		count := 0
		for _, e := range entries {
			if isSetIndex(e) && e.Set == setName {
				count++
			}
		}
		if count != want {
			return fmt.Errorf("set %s/%s: %d set indexes in sindex-list, want %d", namespace, setName, count, want)
		}
		return nil
	}
}

func testAccCheckInfoCommandsNoEnableIndexFalse(resourceName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("resource %s not found", resourceName)
		}
		for k, v := range rs.Primary.Attributes {
			if strings.HasPrefix(k, "info_commands.") && strings.Contains(v, "enable-index=false") {
				return fmt.Errorf("info_commands contains enable-index=false: %s", v)
			}
		}
		return nil
	}
}

func testAccWriteSetRecords(t *testing.T, namespace, setName string, n int) { //nolint:unparam // namespace will vary as more tests are added
	t.Helper()
	client, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("failed to get client: %s", err)
	}
	defer client.Close()

	wp := as.NewWritePolicy(0, 60)
	for i := 0; i < n; i++ {
		key, keyErr := as.NewKey(namespace, setName, fmt.Sprintf("k-%d", i))
		if keyErr != nil {
			t.Fatalf("new key: %s", keyErr)
		}
		if putErr := client.Put(wp, key, as.BinMap{"v": i}); putErr != nil {
			t.Fatalf("put record: %s", putErr)
		}
	}
}

func testAccCheckSetEnableIndex(namespace, setName, want string) resource.TestCheckFunc { //nolint:unparam // namespace will vary as more tests are added
	return func(_ *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		cfg, err := getSetConfig(client, namespace, setName)
		if err != nil {
			return err
		}
		got := cfg["enable-index"]
		if !strings.EqualFold(got, want) {
			return fmt.Errorf("set %s/%s enable-index=%q, want %q", namespace, setName, got, want)
		}
		return nil
	}
}

func testAccCheckSetQueryWorks(namespace, setName string, minRecords int) resource.TestCheckFunc { //nolint:unparam // namespace will vary as more tests are added
	return func(_ *terraform.State) error {
		client, err := testAccGetAerospikeClient()
		if err != nil {
			return err
		}
		defer client.Close()

		stmt := as.NewStatement(namespace, setName)
		rs, err := client.Query(nil, stmt)
		if err != nil {
			return fmt.Errorf("query set %s/%s: %w", namespace, setName, err)
		}
		defer func() { _ = rs.Close() }()

		count := 0
		for rec := range rs.Results() {
			if rec.Err != nil {
				return rec.Err
			}
			count++
		}
		if count < minRecords {
			return fmt.Errorf("query set %s/%s returned %d records, want at least %d", namespace, setName, count, minRecords)
		}
		return nil
	}
}

func testAccSindexConfig(setName, indexName string) string {
	return fmt.Sprintf(`
resource "aerospike_sindex" "test" {
  namespace  = "aerospike"
  set        = "%s"
  name       = "%s"
  index_type = "set"
}
`, setName, indexName)
}

func testAccSindexWithNamespaceConfig(setName, indexName, enableIndex, stopWrites string) string {
	setBlock := fmt.Sprintf(`    "%s" = {`, setName)
	if enableIndex != "" {
		setBlock += fmt.Sprintf(`
      "enable-index"      = "%s"`, enableIndex)
	}
	if stopWrites != "" {
		setBlock += fmt.Sprintf(`
      "stop-writes-count" = "%s"`, stopWrites)
	}
	setBlock += `
    }`

	sindex := ""
	if indexName != "" {
		sindex = fmt.Sprintf(`
resource "aerospike_sindex" "test" {
  namespace  = "aerospike"
  set        = "%s"
  name       = "%s"
  index_type = "set"

  depends_on = [aerospike_namespace_config.test]
}
`, setName, indexName)
	}

	return fmt.Sprintf(`
resource "aerospike_namespace_config" "test" {
  namespace = "aerospike"

  set_config = {
%s
  }
}
%s`, setBlock, sindex)
}

func testAccSindexMixedConfig(configSet, smdSet, smdName string) string {
	return fmt.Sprintf(`
resource "aerospike_namespace_config" "test" {
  namespace = "aerospike"

  set_config = {
    "%s" = {
      "enable-index"      = "true"
      "stop-writes-count" = "50000"
    }
    "%s" = {
      "enable-index"      = "true"
      "stop-writes-count" = "50000"
    }
  }
}

resource "aerospike_sindex" "smd" {
  namespace  = "aerospike"
  set        = "%s"
  name       = "%s"
  index_type = "set"

  depends_on = [aerospike_namespace_config.test]
}
`, configSet, smdSet, smdSet, smdName)
}

func TestAccAerospikeSindex_emptySet(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccSindexPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeSindexDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccSindexConfig("sidx_empty", "sidx_empty-idx"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_sindex.test", "namespace", "aerospike"),
					resource.TestCheckResourceAttr("aerospike_sindex.test", "set", "sidx_empty"),
					resource.TestCheckResourceAttr("aerospike_sindex.test", "name", "sidx_empty-idx"),
					resource.TestCheckResourceAttr("aerospike_sindex.test", "index_type", "set"),
					resource.TestCheckResourceAttr("aerospike_sindex.test", "id", "aerospike/sidx_empty/sidx_empty-idx"),
					resource.TestCheckResourceAttrSet("aerospike_sindex.test", "info_commands.#"),
					testAccCheckSetSindexPresent("aerospike", "sidx_empty", "sidx_empty-idx"),
					testAccCheckSetIndexCount("aerospike", "sidx_empty", 1),
					testAccCheckInfoCommandsNoEnableIndexFalse("aerospike_sindex.test"),
				),
			},
		},
	})
}

func TestAccAerospikeSindex_withData(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccSindexPreCheck(t)
			testAccWriteSetRecords(t, "aerospike", "sidx_data", 20)
		},
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeSindexDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccSindexConfig("sidx_data", "sidx_data-idx"),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckSetSindexPresent("aerospike", "sidx_data", "sidx_data-idx"),
					testAccCheckSetQueryWorks("aerospike", "sidx_data", 20),
				),
			},
		},
	})
}

func TestAccAerospikeSindex_import(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccSindexPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeSindexDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccSindexConfig("sidx_import", "sidx_import-idx"),
			},
			{
				ResourceName:                         "aerospike_sindex.test",
				ImportState:                          true,
				ImportStateVerify:                    true,
				ImportStateId:                        "aerospike/sidx_import/sidx_import-idx",
				ImportStateVerifyIdentifierAttribute: "id",
				ImportStateVerifyIgnore:              []string{"info_commands"},
			},
		},
	})
}

func TestAccAerospikeSindex_rename(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccSindexPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeSindexDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccSindexConfig("sidx_rename", "sidx_rename-old"),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckSetSindexPresent("aerospike", "sidx_rename", "sidx_rename-old"),
					testAccCheckSetIndexCount("aerospike", "sidx_rename", 1),
				),
			},
			{
				Config: testAccSindexConfig("sidx_rename", "sidx_rename-new"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_sindex.test", "name", "sidx_rename-new"),
					testAccCheckSetSindexPresent("aerospike", "sidx_rename", "sidx_rename-new"),
					testAccCheckSetIndexCount("aerospike", "sidx_rename", 1),
				),
			},
		},
	})
}

func TestAccAerospikeSindex_createDoesNotRename(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccSindexPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeSindexDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccSindexConfig("sidx_norename", "sidx_norename-old"),
				Check:  testAccCheckSetSindexPresent("aerospike", "sidx_norename", "sidx_norename-old"),
			},
			{
				Config: testAccSindexConfig("sidx_norename", "sidx_norename-old") + `
resource "aerospike_sindex" "other" {
  namespace  = "aerospike"
  set        = "sidx_norename"
  name       = "sidx_norename-new"
  index_type = "set"
}
`,
				ExpectError: regexp.MustCompile("already has SMD-owned set index"),
			},
		},
	})
}

func TestAccAerospikeSindex_idempotentReapply(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccSindexPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeSindexDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccSindexConfig("sidx_idem", "sidx_idem-idx"),
				Check:  testAccCheckSetSindexPresent("aerospike", "sidx_idem", "sidx_idem-idx"),
			},
			{
				Config:   testAccSindexConfig("sidx_idem", "sidx_idem-idx"),
				PlanOnly: true,
			},
		},
	})
}

func TestAccAerospikeSindex_convertFromEnableIndex(t *testing.T) {
	const setName = "sidx_convert"

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccSindexPreCheck(t)
			testAccWriteSetRecords(t, "aerospike", setName, 15)
		},
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeSindexDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccSindexWithNamespaceConfig(setName, "", "true", "50000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("aerospike_namespace_config.test", "namespace", "aerospike"),
				),
			},
			{
				Config: testAccSindexWithNamespaceConfig(setName, "sidx_convert-idx", "true", "50000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckSetSindexPresent("aerospike", setName, "sidx_convert-idx"),
					testAccCheckSetIndexCount("aerospike", setName, 1),
					testAccCheckSetQueryWorks("aerospike", setName, 15),
					testAccCheckInfoCommandsNoEnableIndexFalse("aerospike_sindex.test"),
				),
			},
			// Keep enable-index=true after conversion; change another param.
			{
				Config: testAccSindexWithNamespaceConfig(setName, "sidx_convert-idx", "true", "60000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckSetSindexPresent("aerospike", setName, "sidx_convert-idx"),
					testAccCheckSetIndexCount("aerospike", setName, 1),
				),
			},
			// Drop enable-index from HCL; index stays.
			{
				Config: testAccSindexWithNamespaceConfig(setName, "sidx_convert-idx", "", "60000"),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckSetSindexPresent("aerospike", setName, "sidx_convert-idx"),
					testAccCheckSetIndexCount("aerospike", setName, 1),
				),
			},
		},
	})
}

func TestAccAerospikeSindex_enableIndexFalseRejectedWhenSMDOwned(t *testing.T) {
	const setName = "sidx_false_rej"

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccSindexPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeSindexDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccSindexWithNamespaceConfig(setName, "sidx_false_rej-idx", "true", "50000"),
				Check:  testAccCheckSetSindexPresent("aerospike", setName, "sidx_false_rej-idx"),
			},
			{
				Config:      testAccSindexWithNamespaceConfig(setName, "sidx_false_rej-idx", "false", "50000"),
				ExpectError: regexp.MustCompile("Cannot disable SMD-owned set index via enable-index"),
			},
		},
	})
}

func TestAccAerospikeSindex_mixedOwnership(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccSindexPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckAerospikeSindexDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccSindexMixedConfig("sidx_mix_cfg", "sidx_mix_smd", "sidx_mix_smd-idx"),
				Check: resource.ComposeAggregateTestCheckFunc(
					testAccCheckSetSindexPresent("aerospike", "sidx_mix_smd", "sidx_mix_smd-idx"),
					testAccCheckSetSindexAbsent("aerospike", "sidx_mix_cfg"),
					testAccCheckSetEnableIndex("aerospike", "sidx_mix_cfg", "true"),
				),
			},
		},
	})
}

func TestAccAerospikeSindex_invalidNamespace(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccSindexPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: `
resource "aerospike_sindex" "test" {
  namespace  = "nonexistent_namespace_xyz"
  set        = "sidx_ns"
  name       = "sidx_ns-idx"
  index_type = "set"
}
`,
				ExpectError: regexp.MustCompile("Namespace not found"),
			},
		},
	})
}

func TestAccDeleteSetSindexConfigOwned(t *testing.T) {
	testAccSindexPreCheck(t)

	client, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("client: %s", err)
	}
	defer client.Close()

	const ns = "aerospike"
	const setName = "sidx_cfg_owned"
	testAccWriteSetRecords(t, ns, setName, 1)

	if _, err := setNamespaceSetParam(client, ns, setName, "enable-index", "true"); err != nil {
		t.Fatalf("enable-index=true: %s", err)
	}

	_, err = deleteSetSindex(client, ns, setName, "does-not-matter")
	if err == nil {
		t.Fatal("expected error deleting config-owned set index via sindex-delete")
	}
	if !strings.Contains(err.Error(), "config-owned") {
		t.Fatalf("expected config-owned error, got: %s", err)
	}

	if _, err := setNamespaceSetParam(client, ns, setName, "enable-index", "false"); err != nil {
		t.Fatalf("cleanup enable-index=false: %s", err)
	}
}

func TestAccCreateSetSindexPrivilegeDenied(t *testing.T) {
	testAccSindexPreCheck(t)

	admin, err := testAccGetAerospikeClient()
	if err != nil {
		t.Fatalf("admin client: %s", err)
	}
	defer admin.Close()

	const user = "sindex_nopriv"
	const pass = "sindex_nopriv" //nolint:gosec // test-only password for a local Docker cluster user
	adminPol := as.NewAdminPolicy()
	_ = admin.DropUser(adminPol, user)
	if createErr := admin.CreateUser(adminPol, user, pass, []string{"read"}); createErr != nil {
		t.Fatalf("create limited user: %s", createErr)
	}
	defer func() { _ = admin.DropUser(adminPol, user) }()

	host := withEnvironmentOverrideString("localhost", "AEROSPIKE_HOST")
	port := withEnvironmentOverrideInt64(3000, "AEROSPIKE_PORT")
	cp := as.NewClientPolicy()
	cp.User = user
	cp.Password = pass
	cp.UseServicesAlternate = true
	limited, err := as.NewClientWithPolicyAndHost(cp, as.NewHost(host, int(port)))
	if err != nil {
		t.Fatalf("limited client: %s", err)
	}
	defer limited.Close()

	_, err = createSetSindex(limited, "aerospike", "sidx_priv", "sidx_priv-idx")
	if err == nil {
		t.Fatal("expected privilege error creating set index")
	}
	if !isPrivError(err) && !strings.Contains(strings.ToLower(err.Error()), "sindex-admin") {
		t.Fatalf("expected privilege / sindex-admin error, got: %s", err)
	}
}
