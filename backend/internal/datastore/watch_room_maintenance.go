package datastore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/models"
)

// StartMaintenance starts the actual owned DATA housekeeping worker once. It
// does not create a user principal, native admission, or source/media owner.
func (a *WatchRoomAuthority) StartMaintenance() error {
	if !a.valid() {
		return ErrWatchRoomUnavailable
	}
	a.mu.Lock()
	if a.closed || a.ctx.Err() != nil {
		a.mu.Unlock()
		return ErrWatchRoomUnavailable
	}
	if a.maintenanceStarted {
		a.mu.Unlock()
		return nil
	}
	a.maintenanceStarted = true
	a.maintenanceDone = make(chan struct{})
	a.operations.Add(1)
	a.mu.Unlock()
	go func() {
		defer a.operations.Done()
		defer close(a.maintenanceDone)
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		a.maintenanceTick()
		for {
			select {
			case <-a.ctx.Done():
				return
			case <-tick.C:
				a.maintenanceTick()
			}
		}
	}()
	return nil
}
func (a *WatchRoomAuthority) maintenanceTick() {
	for i := 0; i < 4 && a.ctx.Err() == nil; i++ {
		a.mu.Lock()
		cursor := a.maintenanceCursor
		a.mu.Unlock()
		next, count, _, _, e := a.maintenanceBatch(cursor)
		if e != nil {
			return
		}
		a.mu.Lock()
		a.maintenanceCursor = next
		a.mu.Unlock()
		if count < 64 {
			return
		}
	}
}

type nativeMaintenanceRoom struct {
	id, profile, inc, physical, status string
	created, expires                   time.Time
	ended                              *time.Time
	host, latest                       time.Time
	members                            int
}

// Each bounded batch owns the real original runtime/settings/SQL lifetime.
// Counters are returned only after known ACK and physical transaction cleanup.
func (a *WatchRoomAuthority) maintenanceBatch(cursor string) (next string, visited int, ended int64, deleted int64, err error) {
	if !a.valid() || len(cursor) > 256 {
		return "", 0, 0, 0, ErrWatchRoomUnavailable
	}
	a.mu.Lock()
	if a.closed || a.ctx.Err() != nil || a.active >= 32 {
		a.mu.Unlock()
		return "", 0, 0, 0, ErrWatchRoomBusy
	}
	a.active++
	a.operations.Add(1)
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.active--
		a.mu.Unlock()
		a.operations.Done()
		if err != nil {
			ended = 0
			deleted = 0
		}
	}()
	ctx, cancel := context.WithTimeout(a.ctx, 2*time.Second)
	defer cancel()
	policy, e := a.security.CapturePolicy(ctx)
	if e != nil {
		return "", 0, 0, 0, ErrWatchRoomUnavailable
	}
	g, e := a.store.beginSettingsProgressTx(ctx, a.coordinator)
	if e != nil {
		return "", 0, 0, 0, ErrWatchRoomUnavailable
	}
	defer func() {
		workErr := g.ctx.Err()
		closeErr := finishShareCreationTransaction(g)
		if err == nil && closeErr != nil {
			err = errors.Join(ErrWatchRoomUnavailable, closeErr)
		}
		if err == nil && (workErr != nil || ctx.Err() != nil || a.ctx.Err() != nil) {
			err = ErrWatchRoomChanged
		}
	}()
	rows, e := g.tx.Query(g.ctx, `SELECT r.id,CASE WHEN octet_length(r.creator_profile_id) BETWEEN 1 AND 256 THEN r.creator_profile_id ELSE NULL END,r.room_incarnation::text,concat(r.tableoid::oid,':',r.xmin::text,':',r.ctid::text),r.status,r.created_at,r.expires_at,r.ended_at,
 (SELECT count(*) FROM (SELECT 1 FROM watch_room_members m WHERE m.room_id=r.id LIMIT 257) bounded_native)+(SELECT count(*) FROM (SELECT 1 FROM watch_room_guest_members q WHERE q.room_id=r.id LIMIT 257) bounded_guests)
 FROM watch_rooms r WHERE r.id>$1 AND octet_length(r.id) BETWEEN 1 AND 256 AND octet_length(r.status)<=16 ORDER BY r.id LIMIT 64 FOR UPDATE OF r`, cursor)
	if e != nil {
		return "", 0, 0, 0, ErrWatchRoomUnavailable
	}
	rooms := make([]nativeMaintenanceRoom, 0, 64)
	for rows.Next() {
		var room nativeMaintenanceRoom
		if e = rows.Scan(&room.id, &room.profile, &room.inc, &room.physical, &room.status, &room.created, &room.expires, &room.ended, &room.members); e != nil || len(rooms) >= 64 {
			rows.Close()
			return "", 0, 0, 0, ErrWatchRoomUnavailable
		}
		rooms = append(rooms, room)
	}
	rows.Close()
	if rows.Err() != nil {
		return "", 0, 0, 0, ErrWatchRoomUnavailable
	}
	if shareLockPolicyRelations(g.ctx, g.tx) != nil || shareLockRelations(g.ctx, g.tx, []string{"watch_rooms", "watch_room_provenance", "watch_room_members", "watch_room_invites", "watch_room_native_admissions", "watch_room_guest_members", "watch_room_account_invites", "watch_room_account_invite_provenance", "watch_party_guest_bindings", "watch_party_playback_bindings"}) != nil {
		return "", 0, 0, 0, ErrWatchRoomUnavailable
	}
	fence := func() error {
		if g.ctx.Err() != nil || a.ctx.Err() != nil {
			return ErrWatchRoomChanged
		}
		bound, e := a.security.BindPolicy(g.ctx, policy, g.hold)
		if e != nil {
			return ErrWatchRoomChanged
		}
		if _, e = bound.EnforceProfilePins(); e != nil {
			return ErrWatchRoomChanged
		}
		return nil
	}
	if e = fence(); e != nil {
		return "", 0, 0, 0, e
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	cutoff := now.Add(-2 * time.Minute)
	deleteBefore := now.Add(-24 * time.Hour)
	for _, room := range rooms {
		room.host, room.latest = room.created, room.created
		if room.members <= 256 {
			// Count before aggregating any activity and re-read it only under the
			// actual current table guards; no unbounded activity snapshot is copied.
			if e = g.tx.QueryRow(g.ctx, `SELECT COALESCE(max(m.last_seen_at) FILTER(WHERE m.profile_id=$2 AND n.admission_kind='creator'),$3::timestamptz),COALESCE(max(m.last_seen_at),$3::timestamptz) FROM watch_room_members m JOIN watch_room_native_admissions n ON n.room_id=m.room_id AND n.profile_id=m.profile_id AND n.member_incarnation=m.member_incarnation AND n.member_joined_at=m.joined_at AND n.room_incarnation=$4::uuid JOIN users u ON u.id=n.profile_id AND u.share_incarnation=n.profile_incarnation AND u.account_id=n.account_id JOIN accounts a ON a.id=n.account_id AND a.share_incarnation=n.account_incarnation WHERE m.room_id=$1`, room.id, room.profile, room.created, room.inc).Scan(&room.host, &room.latest); e != nil {
				return "", 0, 0, 0, ErrWatchRoomUnavailable
			}
		}
		if !shareCanonicalUUID(room.inc) {
			return "", 0, 0, 0, ErrWatchRoomUnavailable
		}
		if room.ended != nil && room.ended.Before(deleteBefore) {
			tag, e := g.tx.Exec(g.ctx, `DELETE FROM watch_rooms WHERE id=$1 AND room_incarnation=$2::uuid AND concat(tableoid::oid,':',xmin::text,':',ctid::text)=$3 AND ended_at<$4`, room.id, room.inc, room.physical, deleteBefore)
			if e != nil {
				return "", 0, 0, 0, ErrWatchRoomUnavailable
			}
			deleted += tag.RowsAffected()
			continue
		}
		if room.status == models.WatchRoomStatusEnded {
			continue
		}
		reason := ""
		if !room.expires.After(now) {
			reason = "expired"
		} else if room.members <= 256 && room.host.Before(cutoff) {
			// Only a genuine original native host can keep a managed room alive.
			// Unattested legacy rows are not adopted from current matching IDs.
			var original bool
			if e = g.tx.QueryRow(g.ctx, `SELECT EXISTS(SELECT 1 FROM watch_room_provenance p JOIN watch_room_native_admissions n ON n.room_id=p.room_id AND n.profile_id=p.creator_profile_id AND n.admission_kind='creator' AND n.version=1 AND n.room_incarnation=p.room_incarnation WHERE p.room_id=$1 AND p.room_incarnation=$2::uuid)`, room.id, room.inc).Scan(&original); e != nil {
				return "", 0, 0, 0, ErrWatchRoomUnavailable
			}
			if original {
				reason = "host_disconnected"
				if room.latest.Before(cutoff) {
					reason = "all_left"
				}
			}
		}
		if reason != "" {
			tag, e := g.tx.Exec(g.ctx, `UPDATE watch_rooms SET status='ended',revision=revision+1,anchor_updated_at=$4,ended_at=$4,end_reason=$5 WHERE id=$1 AND room_incarnation=$2::uuid AND concat(tableoid::oid,':',xmin::text,':',ctid::text)=$3 AND status<>'ended'`, room.id, room.inc, room.physical, now, reason)
			if e != nil {
				return "", 0, 0, 0, ErrWatchRoomUnavailable
			}
			ended += tag.RowsAffected()
		}
	}
	if e = fence(); e != nil {
		return "", 0, 0, 0, e
	}
	if e = g.tx.Commit(g.ctx); e != nil {
		if errors.Is(e, pgx.ErrTxCommitRollback) {
			return "", 0, 0, 0, ErrWatchRoomUnavailable
		}
		return "", 0, 0, 0, ErrWatchRoomUnknown
	}
	next = ""
	if len(rooms) == 64 {
		next = rooms[len(rooms)-1].id
	}
	return next, len(rooms), ended, deleted, nil
}
