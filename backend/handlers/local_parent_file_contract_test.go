package handlers

import (
	"net/http"
	"novastream/api/openapi"
	"strings"
	"testing"
)

// Every new operation participates in the existing global operation collector.
// This default contract path needs no PG and must fail unavailable, not fall
// back to a generic provider or legacy child policy.
func parentLocalFileUnavailableContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	var h *ParentLocalFileHandler
	t.Run("parent local file unavailable operations", func(t *testing.T) {
		exercise(t, v, contractCall{method: http.MethodPost, target: "/api/users/child/kids/local-file-approvals", vars: map[string]string{"userID": "child"}, body: map[string]string{"itemId": "local-item"}}, h.Approve, 503)
		path := "/api/users/child/library/items/local-item/approved-file?approvalId=lfa_" + strings.Repeat("a", 64)
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodDelete} {
			handler := h.Stream
			if method == http.MethodDelete {
				handler = h.Revoke
			}
			exercise(t, v, contractCall{method: method, target: path, vars: map[string]string{"userID": "child", "itemID": "local-item"}}, handler, 503)
		}
	})
}
