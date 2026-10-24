package main

import "testing"

func TestChaosTargetsNeedTheControlToken(t *testing.T) {
	if _, _, err := chaosTargets("a=http://dim-a:8081", ""); err == nil {
		t.Fatal("targets were accepted without a control token")
	}
	names, controls, err := chaosTargets("a=http://dim-a:8081, b=http://dim-b:8081", "control-token")
	if err != nil || len(names) != 2 || controls["b"].Token != "control-token" {
		t.Fatalf("names %v, controls %v, err %v", names, controls, err)
	}
	if _, _, err := chaosTargets("", ""); err != nil {
		t.Fatalf("no targets and no token: %v", err)
	}
}
