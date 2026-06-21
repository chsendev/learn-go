package myhttp

import (
	"fmt"
	"strings"
	"testing"
)

func TestName(t *testing.T) {
	n := strings.SplitN("1: 2: 3", ": ", 2)
	fmt.Println(n[0])
	fmt.Println(n[1])
}
