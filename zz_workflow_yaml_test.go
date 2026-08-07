package jobs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Every GitHub Actions workflow must parse, with NO DUPLICATE MAPPING KEYS.
//
// This exists because a duplicate key is the one YAML error that looks like
// success. Adding a second `if:` to a step that already had one produced this:
//
//	if: matrix.tz == ''
//	uses: actions/upload-artifact@...
//	if: always()
//
// `yaml.Unmarshal` into a generic map accepts it — last key wins — so a naive
// "does it parse?" check reports OK. GitHub does not: it rejected the whole file,
// and NO JOBS RAN. On a feature branch that shows up as a run named after the file
// path (".github/workflows/ci.yml") instead of the workflow ("CI"), which is easy
// to mistake for a normal failure. On main it means the release job never fires.
//
// The lenient outcome is no better: last-wins silently discards the FIRST condition,
// so the guard a comment claims to provide is not there.
//
// A guard living inside the workflows cannot catch this — an unparseable workflow
// does not run. So it lives here, in the Go suite, where it fails before the push.
//
// yaml.v3's KnownFields does not cover duplicate keys, so this walks the node tree
// directly rather than unmarshalling.
func TestWorkflowYAMLHasNoDuplicateKeys(t *testing.T) {
	paths, err := filepath.Glob(".github/workflows/*.yml")
	require.NoError(t, err)
	more, err := filepath.Glob(".github/workflows/*.yaml")
	require.NoError(t, err)
	paths = append(paths, more...)

	require.NotEmpty(t, paths,
		"no workflow files found — if they moved, this guard is silently testing nothing")

	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			require.NoError(t, err)

			var root yaml.Node
			require.NoError(t, yaml.Unmarshal(data, &root),
				"%s does not parse as YAML; GitHub will reject the file and run NO jobs from it", path)

			var walk func(n *yaml.Node, where string)
			walk = func(n *yaml.Node, where string) {
				if n.Kind == yaml.MappingNode {
					seen := map[string]int{}
					// Mapping content alternates key, value, key, value...
					for i := 0; i+1 < len(n.Content); i += 2 {
						k := n.Content[i]
						if prev, dup := seen[k.Value]; dup {
							t.Errorf("%s:%d duplicate key %q in %s (first seen at line %d).\n"+
								"GitHub rejects the whole workflow — no jobs run from this file at all, "+
								"and on main the release job never fires. If you meant to combine two "+
								"conditions, AND them into one key: `if: always() && matrix.tz == ''`.",
								path, k.Line, k.Value, where, prev)
						}
						seen[k.Value] = k.Line
						walk(n.Content[i+1], where+"."+k.Value)
					}
					return
				}
				for idx, c := range n.Content {
					child := where
					if n.Kind == yaml.SequenceNode {
						child = where + "[" + itoa(idx) + "]"
					}
					walk(c, child)
				}
			}
			walk(&root, strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)))
		})
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
