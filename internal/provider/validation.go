// Copyright (c) Harel Safra
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"github.com/hashicorp/terraform-plugin-framework/diag"
)

// IncompatibilityRule defines a constraint between parameters.
// Check returns an error message if the rule is violated, or nil if OK.
type IncompatibilityRule struct {
	Description string
	Check       func(params map[string]string) *string
}

// ValidateParamCompatibility runs all rules against the given params and returns
// diagnostics for any violations. This is a general-purpose framework that can
// be extended with new rules for any config context.
func ValidateParamCompatibility(params map[string]string, rules []IncompatibilityRule) diag.Diagnostics {
	var diags diag.Diagnostics
	for _, rule := range rules {
		if msg := rule.Check(params); msg != nil {
			diags.AddError("Parameter incompatibility: "+rule.Description, *msg)
		}
	}
	return diags
}

// xdrReservedSetPolicyKeys are XDR namespace-level parameters that must be managed
// via the set_policy block, not the params map.
var xdrReservedSetPolicyKeys = map[string]bool{
	"ship-only-specified-sets": true,
	"ship-set":                 true,
	"ignore-set":               true,
}
