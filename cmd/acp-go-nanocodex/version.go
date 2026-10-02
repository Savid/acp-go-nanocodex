package main

// buildVersion is the adapter version. It defaults to "dev" and is overridden
// at release time via -ldflags "-X main.buildVersion=<version>".
var buildVersion = "dev"

func version() string {
	if buildVersion == "" {
		return "dev"
	}

	return buildVersion
}
