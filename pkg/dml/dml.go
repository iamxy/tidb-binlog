package dml

import "strings"

// GenColumnPlaceholders generates placeholders in question mark format,like "?,?,?".
func GenColumnPlaceholders(length int) string {
	values := make([]string, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return strings.Join(values, ",")
}
