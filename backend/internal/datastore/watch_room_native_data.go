package datastore

import (
	"encoding/json"
	"math"
	"sort"
	"time"

	"novastream/models"
)

func nativeJSONBytes(value any, limit int) ([]byte, error) {
	raw, e := json.Marshal(value)
	if e != nil || len(raw) > limit {
		return nil, ErrWatchRoomUnavailable
	}
	return raw, nil
}

// This is a private bounded DATA reader. Every caller must first own the actual
// current original room/admission transaction; this helper grants no access.
func (o *watchRoomOperation) nativeRoomData(state *nativeRoomState) (*models.WatchRoom, error) {
	id := state.room.id
	var native, guests int
	var size int64
	e := o.g.tx.QueryRow(o.ctx, `WITH people AS (SELECT creator_profile_id AS profile_id FROM watch_rooms WHERE id=$1 UNION SELECT profile_id FROM watch_room_invites WHERE room_id=$1)
 SELECT (SELECT count(*) FROM people),(SELECT count(*) FROM watch_room_guest_members WHERE room_id=$1),
 6*(octet_length(r.id)+octet_length(r.creator_profile_id)+octet_length(r.title)+octet_length(r.media_type)+octet_length(r.item_id)+octet_length(r.poster_url)+octet_length(r.backdrop_url)+octet_length(r.params::text)+octet_length(r.updated_by)+octet_length(r.end_reason)+
 COALESCE((SELECT sum(octet_length(u.id)+octet_length(u.name)+octet_length(u.color)+octet_length(u.icon_url)+octet_length(COALESCE(m.client_id,''))+octet_length(COALESCE(m.capabilities,'{}'::jsonb)::text)) FROM people p JOIN users u ON u.id=p.profile_id LEFT JOIN watch_room_members m ON m.room_id=r.id AND m.profile_id=p.profile_id),0)+
 COALESCE((SELECT sum(octet_length(g.guest_id)+octet_length(g.name)+octet_length(g.client_id)+octet_length(g.capabilities::text)) FROM watch_room_guest_members g WHERE g.room_id=r.id),0))+
 2048*((SELECT count(*) FROM people)+(SELECT count(*) FROM watch_room_guest_members WHERE room_id=r.id))+65536 FROM watch_rooms r WHERE r.id=$1`, id).Scan(&native, &guests, &size)
	if e != nil || native < 1 || native > 256 || guests < 0 || guests > 256 || native+guests > 512 || size < 0 || size > 1<<20 {
		return nil, ErrWatchRoomUnavailable
	}
	var originalCount int
	e = o.g.tx.QueryRow(o.ctx, `SELECT count(*) FROM watch_room_native_admissions n JOIN users u ON u.id=n.profile_id AND u.share_incarnation=n.profile_incarnation AND u.account_id=n.account_id JOIN accounts a ON a.id=n.account_id AND a.share_incarnation=n.account_incarnation WHERE n.room_id=$1 AND n.version=1 AND n.room_incarnation=$2::uuid AND n.expires_at=$3 AND ((n.member_incarnation IS NULL AND NOT EXISTS(SELECT 1 FROM watch_room_members m WHERE m.room_id=n.room_id AND m.profile_id=n.profile_id)) OR EXISTS(SELECT 1 FROM watch_room_members m WHERE m.room_id=n.room_id AND m.profile_id=n.profile_id AND m.member_incarnation=n.member_incarnation AND m.joined_at=n.member_joined_at)) AND ((n.admission_kind='creator' AND n.profile_id=$4 AND n.native_invite_receipt IS NULL) OR (n.admission_kind IN ('household','account') AND EXISTS(SELECT 1 FROM watch_room_invites i WHERE i.room_id=n.room_id AND i.profile_id=n.profile_id AND concat(i.tableoid::oid,':',i.xmin::text,':',i.ctid::text)=n.native_invite_receipt)))`, id, state.room.inc, state.room.expires, state.room.profile).Scan(&originalCount)
	if e != nil || originalCount != native {
		return nil, ErrWatchRoomRecreate
	}
	out := &models.WatchRoom{ID: id, CreatorProfileID: state.room.profile, CreatorName: state.creator.name, CreatedAt: state.room.created, ExpiresAt: state.room.expires, Members: make([]models.WatchRoomMember, 0, native+guests)}
	var params []byte
	e = o.g.tx.QueryRow(o.ctx, `SELECT CASE WHEN octet_length(title)<=4096 THEN title ELSE NULL END,CASE WHEN octet_length(media_type)<=64 THEN media_type ELSE NULL END,CASE WHEN octet_length(item_id)<=1024 THEN item_id ELSE NULL END,CASE WHEN octet_length(poster_url)<=8192 THEN poster_url ELSE NULL END,CASE WHEN octet_length(backdrop_url)<=8192 THEN backdrop_url ELSE NULL END,CASE WHEN octet_length(params::text)<=1048576 THEN params ELSE NULL END,status,waiting_for_ready,position,duration,revision,CASE WHEN octet_length(updated_by)<=262 THEN updated_by ELSE NULL END,anchor_updated_at,ended_at,CASE WHEN octet_length(end_reason)<=64 THEN end_reason ELSE NULL END FROM watch_rooms WHERE id=$1`, id).Scan(&out.Title, &out.MediaType, &out.ItemID, &out.PosterURL, &out.BackdropURL, &params, &out.Status, &out.WaitingForReady, &out.Position, &out.Duration, &out.Revision, &out.UpdatedBy, &out.AnchorUpdatedAt, &out.EndedAt, &out.EndReason)
	if e != nil || json.Unmarshal(params, &out.Params) != nil || math.IsNaN(out.Position) || math.IsInf(out.Position, 0) || math.IsNaN(out.Duration) || math.IsInf(out.Duration, 0) {
		return nil, ErrWatchRoomUnavailable
	}
	rows, e := o.g.tx.Query(o.ctx, `WITH people AS (SELECT creator_profile_id AS profile_id FROM watch_rooms WHERE id=$1 UNION SELECT profile_id FROM watch_room_invites WHERE room_id=$1)
 SELECT false,CASE WHEN octet_length(u.id) BETWEEN 1 AND 256 THEN u.id ELSE NULL END,CASE WHEN octet_length(u.name)<=4096 THEN u.name ELSE NULL END,CASE WHEN octet_length(u.color)<=64 THEN u.color ELSE NULL END,CASE WHEN octet_length(u.icon_url)<=8192 THEN u.icon_url ELSE NULL END,CASE WHEN octet_length(COALESCE(m.client_id,''))<=4096 THEN COALESCE(m.client_id,'') ELSE NULL END,p.profile_id=r.creator_profile_id,COALESCE(m.ready,false),COALESCE(m.buffering,false),m.profile_id IS NOT NULL,COALESCE(m.joined_at,r.created_at),COALESCE(m.last_seen_at,r.created_at-interval '1 day'),CASE WHEN octet_length(COALESCE(m.capabilities,'{}'::jsonb)::text)<=4096 THEN COALESCE(m.capabilities,'{}'::jsonb) ELSE NULL END
 FROM people p JOIN watch_rooms r ON r.id=$1 JOIN users u ON u.id=p.profile_id LEFT JOIN watch_room_members m ON m.room_id=r.id AND m.profile_id=p.profile_id
 UNION ALL SELECT true,CASE WHEN octet_length(guest_id) BETWEEN 1 AND 256 THEN 'guest:'||guest_id ELSE NULL END,CASE WHEN octet_length(name)<=160 THEN name ELSE NULL END,'','',CASE WHEN octet_length(client_id)<=4096 THEN client_id ELSE NULL END,false,ready,buffering,true,joined_at,last_seen_at,CASE WHEN octet_length(capabilities::text)<=4096 THEN capabilities ELSE NULL END FROM watch_room_guest_members WHERE room_id=$1`, id)
	if e != nil {
		return nil, ErrWatchRoomUnavailable
	}
	defer rows.Close()
	now := time.Now().UTC()
	for rows.Next() {
		if len(out.Members) >= 512 {
			return nil, ErrWatchRoomUnavailable
		}
		var m models.WatchRoomMember
		var caps []byte
		if e = rows.Scan(&m.IsGuest, &m.ProfileID, &m.Name, &m.Color, &m.IconURL, &m.ClientID, &m.IsCreator, &m.Ready, &m.Buffering, &m.Joined, &m.JoinedAt, &m.LastSeenAt, &caps); e != nil || json.Unmarshal(caps, &m.Capabilities) != nil {
			return nil, ErrWatchRoomUnavailable
		}
		m.Present = now.Sub(m.LastSeenAt) <= 15*time.Second
		out.Members = append(out.Members, m)
	}
	if rows.Err() != nil || len(out.Members) != native+guests {
		return nil, ErrWatchRoomUnavailable
	}
	sort.SliceStable(out.Members, func(i, j int) bool {
		a, b := out.Members[i], out.Members[j]
		if a.IsGuest != b.IsGuest {
			return !a.IsGuest
		}
		if a.IsCreator != b.IsCreator {
			return a.IsCreator
		}
		if !a.JoinedAt.Equal(b.JoinedAt) {
			return a.JoinedAt.Before(b.JoinedAt)
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.ProfileID < b.ProfileID
	})
	if out.Status == models.WatchRoomStatusPlaying && !out.WaitingForReady {
		out.Position += now.Sub(out.AnchorUpdatedAt).Seconds()
		if out.Duration > 0 && out.Position > out.Duration {
			out.Position = out.Duration
		}
	}
	if _, e = nativeJSONBytes(out, 1<<20); e != nil {
		return nil, e
	}
	return out, nil
}

func (o *watchRoomOperation) nativeRoomsDataBudget(ids []string) error {
	var size int64
	e := o.g.tx.QueryRow(o.ctx, `WITH selected AS (SELECT * FROM watch_rooms WHERE id=ANY($1::text[])),people AS (SELECT id AS room_id,creator_profile_id AS profile_id FROM selected UNION SELECT i.room_id,i.profile_id FROM watch_room_invites i JOIN selected r ON r.id=i.room_id)
 SELECT 65536+2048*((SELECT count(*) FROM selected)+(SELECT count(*) FROM people)+(SELECT count(*) FROM watch_room_guest_members g JOIN selected r ON r.id=g.room_id))+
 6*(COALESCE((SELECT sum(octet_length(r.id)+octet_length(r.creator_profile_id)+octet_length(r.title)+octet_length(r.media_type)+octet_length(r.item_id)+octet_length(r.poster_url)+octet_length(r.backdrop_url)+octet_length(r.params::text)+octet_length(r.updated_by)+octet_length(r.end_reason)) FROM selected r),0)+
 COALESCE((SELECT sum(octet_length(u.id)+octet_length(u.name)+octet_length(u.color)+octet_length(u.icon_url)+octet_length(COALESCE(m.client_id,''))+octet_length(COALESCE(m.capabilities,'{}'::jsonb)::text)) FROM people p JOIN users u ON u.id=p.profile_id LEFT JOIN watch_room_members m ON m.room_id=p.room_id AND m.profile_id=p.profile_id),0)+
 COALESCE((SELECT sum(octet_length(g.guest_id)+octet_length(g.name)+octet_length(g.client_id)+octet_length(g.capabilities::text)) FROM watch_room_guest_members g JOIN selected r ON r.id=g.room_id),0))`, ids).Scan(&size)
	if e != nil || size < 0 || size > 1<<20 {
		return ErrWatchRoomUnavailable
	}
	return nil
}
