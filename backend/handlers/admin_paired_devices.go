package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/models"
)

type pairedDeviceInvite struct {
	ID             string     `json:"id"`
	ConnectionCode string     `json:"connectionCode"`
	PeerName       string     `json:"peerName,omitempty"`
	ClaimedAt      *time.Time `json:"claimedAt"`
	RevokedAt      *time.Time `json:"revokedAt,omitempty"`
}

type pairedDeviceView struct {
	PeerID             string               `json:"peerId"`
	Device             *models.Client       `json:"device,omitempty"`
	Invites            []pairedDeviceInvite `json:"invites"`
	ActiveSessionCount int                  `json:"activeSessionCount"`
	Revoked            bool                 `json:"revoked"`
	LastClaimedAt      time.Time            `json:"lastClaimedAt"`
}

// PairedDevicesPage serves the claimed Iroh device administration page.
func (h *AdminUIHandler) PairedDevicesPage(w http.ResponseWriter, r *http.Request) {
	isAdmin, accountID, basePath, username := h.getPageRoleInfo(r)
	settings, err := config.NewManager(h.settingsPath).Load()
	if err != nil {
		http.Error(w, "Failed to load settings", http.StatusInternalServerError)
		return
	}
	data := AdminPageData{
		CurrentPath: basePath + "/paired-devices", BasePath: basePath,
		ServerBasePath: h.serverBasePath, IsAdmin: isAdmin, AccountID: accountID,
		Username: username, Settings: settings, Version: GetBackendVersion(), BuildID: GetBackendBuildID(),
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if h.pairedDevicesTemplate == nil {
		http.Error(w, "Paired devices template not loaded", http.StatusInternalServerError)
		return
	}
	if err := h.pairedDevicesTemplate.ExecuteTemplate(w, "base", data); err != nil {
		fmt.Printf("Paired devices template error: %v\n", err)
	}
}

// GetPairedDevices returns claimed codes grouped by stable app device ID.
func (h *AdminUIHandler) GetPairedDevices(w http.ResponseWriter, r *http.Request) {
	if h.remoteAccessService == nil {
		http.Error(w, `{"error":"remote access is unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	invites, err := h.remoteAccessService.ListInvites(r.Context())
	if err != nil {
		http.Error(w, `{"error":"failed to load paired devices"}`, http.StatusInternalServerError)
		return
	}
	byPeer := make(map[string]*pairedDeviceView)
	for _, inv := range invites {
		peerID := strings.TrimSpace(inv.UsedByPeerID)
		if inv.UsedAt == nil || peerID == "" {
			continue
		}
		entry := byPeer[peerID]
		if entry == nil {
			entry = &pairedDeviceView{PeerID: peerID, Revoked: true}
			byPeer[peerID] = entry
			if h.clientsService != nil {
				if device, getErr := h.clientsService.Get(peerID); getErr == nil {
					entry.Device = device
				}
			}
			if h.sessionsService != nil {
				entry.ActiveSessionCount = len(h.sessionsService.GetSessionsForClient(peerID))
			}
		}
		entry.Invites = append(entry.Invites, pairedDeviceInvite{
			ID: inv.ID, ConnectionCode: inv.ConnectionCode, PeerName: inv.PeerName,
			ClaimedAt: inv.UsedAt, RevokedAt: inv.RevokedAt,
		})
		if inv.RevokedAt == nil {
			entry.Revoked = false
		}
		if inv.UsedAt.After(entry.LastClaimedAt) {
			entry.LastClaimedAt = *inv.UsedAt
		}
	}
	result := make([]pairedDeviceView, 0, len(byPeer))
	for _, entry := range byPeer {
		sort.Slice(entry.Invites, func(i, j int) bool {
			return entry.Invites[i].ClaimedAt.After(*entry.Invites[j].ClaimedAt)
		})
		result = append(result, *entry)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].LastClaimedAt.After(result[j].LastClaimedAt) })
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// RevokePairedDevice disables every pairing and app session for one device.
func (h *AdminUIHandler) RevokePairedDevice(w http.ResponseWriter, r *http.Request) {
	if h.remoteAccessService == nil || h.sessionsService == nil {
		http.Error(w, `{"error":"paired device management is unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	inviteID := strings.TrimSpace(mux.Vars(r)["inviteID"])
	invites, err := h.remoteAccessService.ListInvites(r.Context())
	if err != nil {
		http.Error(w, `{"error":"failed to load paired device"}`, http.StatusInternalServerError)
		return
	}
	peerID := ""
	for _, inv := range invites {
		if inv.ID == inviteID && inv.UsedAt != nil {
			peerID = strings.TrimSpace(inv.UsedByPeerID)
			break
		}
	}
	if peerID == "" {
		http.Error(w, `{"error":"paired device not found"}`, http.StatusNotFound)
		return
	}
	// These are two distinct operations. Either can have durable partial effects;
	// no transaction or retry spanning invites and sessions is claimed.
	revokedInvites, inviteErr := h.remoteAccessService.RevokePeer(r.Context(), peerID)
	var revokedSessions int64
	var sessionErr error
	if h.sessionsService.UsesPostgres() {
		revokedSessions, sessionErr = h.sessionsService.RevokeAllForClientContext(r.Context(), peerID)
	} else {
		// Explicit configured file/memory compatibility, never a PG-error fallback.
		revokedSessions = int64(h.sessionsService.RevokeAllForClient(peerID))
	}
	if r.Context().Err() != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	result := struct {
		OK              bool   `json:"ok"`
		PeerID          string `json:"peerId"`
		InviteOutcome   string `json:"inviteOutcome"`
		SessionOutcome  string `json:"sessionOutcome"`
		RevokedInvites  int    `json:"revokedInvites"`
		RevokedSessions *int64 `json:"revokedSessions,omitempty"`
		OutcomeUnknown  bool   `json:"outcomeUnknown,omitempty"`
		Error           string `json:"error,omitempty"`
	}{OK: inviteErr == nil && sessionErr == nil, PeerID: peerID, InviteOutcome: "known", SessionOutcome: "known", RevokedInvites: revokedInvites}
	if inviteErr != nil {
		// Count is only the completed updates reported by RevokePeer; failure
		// does not prove that all remaining invites or pairings stayed unchanged.
		result.InviteOutcome = "failed"
	}
	if sessionErr == nil {
		result.RevokedSessions = &revokedSessions
	} else if errors.Is(sessionErr, datastore.ErrSessionTargetRevokeUnknown) {
		result.SessionOutcome = "unknown"
		result.OutcomeUnknown = true
	} else {
		result.SessionOutcome = "unavailable"
	}
	w.Header().Set("Content-Type", "application/json")
	if !result.OK {
		result.Error = "Paired device revocation could not be fully confirmed"
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(result)
}
