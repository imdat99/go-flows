#!/usr/bin/env bash
# Tests release-check.sh against throwaway repositories.
# Run: bash .github/scripts/release-check_test.sh
set -euo pipefail
script="$(cd "$(dirname "$0")" && pwd)/release-check.sh"
fail=0

changelog='# Changelog

## Unreleased

## v0.2.0 (2026-12-01)

### Changed

- Something.

## v0.1.1

## otelflows/v0.1.0

First otelflows release.

## v0.1.0 (2026-11-16)

First release.
'

# check NAME WANT_STATUS WANT_OUTPUT MODULE VERSION [setup commands...]:
# WANT_OUTPUT is the printed tag (status 0) or a part of the error.
check() {
	local name=$1 want_status=$2 want=$3 module=$4 version=$5
	shift 5
	local dir
	dir=$(mktemp -d)
	(
		cd "$dir"
		git init -q
		git config user.email t@example.com
		git config user.name t
		git config tag.gpgSign false
		git config commit.gpgSign false
		printf '%s' "$changelog" >CHANGELOG.md
		mkdir otelflows analyzer
		printf 'module github.com/imdat99/go-flows/otelflows\n\nrequire (\n\tgithub.com/imdat99/go-flows v0.1.0\n\tgo.opentelemetry.io/otel v1.47.0\n)\n' >otelflows/go.mod
		printf 'module github.com/imdat99/go-flows/analyzer\n\nrequire golang.org/x/tools v0.52.0\n' >analyzer/go.mod
		git add -A
		git commit -q -m init
		git tag v0.1.0
		for cmd in "$@"; do eval "$cmd"; done
	)
	local got status=0
	got=$(cd "$dir" && "$script" "$module" "$version" notes.md 2>&1) || status=$?
	local notes=''
	[[ -f $dir/notes.md ]] && notes=$(cat "$dir/notes.md")
	rm -rf "$dir"
	if [[ $status -ne $want_status ]]; then
		echo "FAIL $name: status $status, want $want_status ($got)"
		fail=1
	elif [[ $want_status -eq 0 && $got != "$want" ]]; then
		echo "FAIL $name: got '$got', want '$want'"
		fail=1
	elif [[ $want_status -ne 0 && $got != *"$want"* ]]; then
		echo "FAIL $name: got '$got', want an error containing '$want'"
		fail=1
	elif [[ $name == notes* && $notes != "$NOTES_WANT" ]]; then
		echo "FAIL $name: notes '$notes', want '$NOTES_WANT'"
		fail=1
	else
		echo "ok   $name"
	fi
}

NOTES_WANT='### Changed

- Something.'
check "notes of a release" 0 v0.2.0 go-flows v0.2.0
NOTES_WANT='First otelflows release.'
check "notes of a submodule" 0 otelflows/v0.1.0 otelflows v0.1.0
check "not semver" 1 "not vMAJOR.MINOR.PATCH" go-flows 0.2.0
check "pre-release" 1 "not vMAJOR.MINOR.PATCH" go-flows v0.2.0-rc.1
check "leading zero" 1 "not vMAJOR.MINOR.PATCH" go-flows v0.02.0
check "tag exists" 1 "already exists" go-flows v0.1.0
check "not greater" 1 "not greater than the latest release v0.3.0" go-flows v0.2.0 "git tag v0.3.0"
check "numeric order" 1 "not greater than the latest release v0.10.0" go-flows v0.9.0 "git tag v0.10.0"
check "pre-release tags ignored" 0 v0.2.0 go-flows v0.2.0 "git tag v0.3.0-rc.1"
check "other modules' tags ignored" 0 v0.2.0 go-flows v0.2.0 "git tag otelflows/v0.5.0"
check "submodule against its own tags" 1 "latest release otelflows/v0.2.0" otelflows v0.1.0 "git tag otelflows/v0.2.0"
check "no section" 1 "no section '## v0.3.0'" go-flows v0.3.0
check "Unreleased is not a version" 1 "not vMAJOR.MINOR.PATCH" go-flows Unreleased
check "empty section" 1 "is empty" go-flows v0.1.1
check "unknown module" 1 "unknown module" bench v0.1.0
check "submodule with replace" 1 "replaces github.com/imdat99/go-flows" otelflows v0.1.0 \
	"printf 'replace github.com/imdat99/go-flows => ../\n' >>otelflows/go.mod"
check "submodule with replace block" 1 "replaces github.com/imdat99/go-flows" otelflows v0.1.0 \
	"printf 'replace (\n\tgithub.com/imdat99/go-flows v0.1.0 => ../\n)\n' >>otelflows/go.mod"
check "submodule requires an untagged version" 1 "v0.0.0, which is not tagged" otelflows v0.1.0 \
	"sed -i 's|go-flows v0.1.0|go-flows v0.0.0|' otelflows/go.mod"
check "submodule requires a pseudo-version" 1 "not a release version" otelflows v0.1.0 \
	"sed -i 's|go-flows v0.1.0|go-flows v0.0.0-20261117040327-35c0a4b7c173|' otelflows/go.mod"
check "submodule without go-flows" 0 analyzer/v0.1.0 analyzer v0.1.0 \
	"printf '## analyzer/v0.1.0\n\nFirst analyzer release.\n' >>CHANGELOG.md"

exit $fail
