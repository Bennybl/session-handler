package repositorytest

import (
	"reflect"
	"testing"
)

func TestCasesExposeTheSharedAdapterContract(t *testing.T) {
	t.Parallel()

	want := []string{
		"lifecycle snapshots",
		"snapshot isolation",
		"atomic mutation rollback",
		"generic state scoped filters",
		"pagination and stable cursors",
		"query result isolation",
		"event ID atomicity and deduplication",
	}

	cases := Cases()
	got := make([]string, 0, len(cases))
	for _, contractCase := range cases {
		got = append(got, contractCase.Name)
		if contractCase.Run == nil {
			t.Fatalf("contract case %q has a nil runner", contractCase.Name)
		}
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Cases() names = %v, want %v", got, want)
	}
}
