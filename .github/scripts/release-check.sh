#!/usr/bin/env bash
# Checks a release before the release workflow tags it, and writes its notes.
#
#   release-check.sh MODULE VERSION NOTES_FILE
#
# MODULE is go-flows (the root module, tag vX.Y.Z), otelflows (tag
# otelflows/vX.Y.Z) or analyzer (tag analyzer/vX.Y.Z). VERSION is vX.Y.Z.
# The checks:
#   - VERSION is a release version vMAJOR.MINOR.PATCH (no pre-release);
#   - the tag does not exist and VERSION is greater than the module's latest
#     release tag;
#   - CHANGELOG.md has a section headed "## <tag>" (a date may follow) that
#     is not empty: its body is written to NOTES_FILE as the release notes;
#   - for a submodule: its go.mod does not replace go-flows (a replace is
#     ignored for consumers) and requires it, if at all, at a released tag.
# Prints the tag. Run from the repository root, with the tags fetched.
# Tests: bash .github/scripts/release-check_test.sh
set -euo pipefail

die() {
	echo "release-check: $*" >&2
	exit 1
}

[[ $# -eq 3 ]] || die "usage: release-check.sh MODULE VERSION NOTES_FILE"
module=$1 version=$2 notes=$3
root=github.com/imdat99/go-flows

case $module in
go-flows) prefix='' dir=. ;;
otelflows | analyzer) prefix=$module/ dir=$module ;;
*) die "unknown module '$module' (go-flows, otelflows or analyzer)" ;;
esac

semver='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
[[ $version =~ $semver ]] || die "version '$version' is not vMAJOR.MINOR.PATCH"
tag=$prefix$version

if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
	die "tag $tag already exists"
fi

# The module's latest release: its tags without the prefix; pre-releases
# and other modules' tags are left out ("v*" does not match "otelflows/v*").
latest=$(git tag -l "${prefix}v*" | sed "s|^$prefix||" | { grep -E "$semver" || true; } | sort -V | tail -n 1)
if [[ -n $latest ]]; then
	top=$(printf '%s\n%s\n' "$latest" "$version" | sort -V | tail -n 1)
	if [[ $top != "$version" || $version == "$latest" ]]; then
		die "version $version is not greater than the latest release $prefix$latest"
	fi
fi

# The section "## <tag>" up to the next "## " heading, without the blank
# lines around it.
[[ -f CHANGELOG.md ]] || die "CHANGELOG.md not found"
body=$(awk -v tag="$tag" '
	/^## / { if (in_s) exit; if ($2 == tag) { in_s = 1; found = 1; next } }
	in_s { print }
	END { if (!found) exit 3 }
' CHANGELOG.md) || die "CHANGELOG.md has no section '## $tag'"
# $(...) drops the trailing blank lines; drop the leading ones.
body=$(printf '%s\n' "$body" | sed -e '/[^[:space:]]/,$!d')
[[ -n $body ]] || die "the CHANGELOG.md section '## $tag' is empty"
printf '%s\n' "$body" >"$notes"

if [[ $dir != . ]]; then
	gomod=$dir/go.mod
	[[ -f $gomod ]] || die "$gomod not found"
	if grep -Eq "^[[:space:]]*(replace[[:space:]]+)?$root([[:space:]]+[^[:space:]]+)?[[:space:]]+=>" "$gomod"; then
		die "$gomod replaces $root: consumers ignore a replace; require a released version"
	fi
	req=$(awk -v m="$root" '$1 == m { print $2 } $1 == "require" && $2 == m { print $3 }' "$gomod")
	if [[ -n $req ]]; then
		[[ $req =~ $semver ]] || die "$gomod requires $root $req, not a release version"
		git rev-parse -q --verify "refs/tags/$req" >/dev/null || die "$gomod requires $root $req, which is not tagged"
	fi
fi

echo "$tag"
