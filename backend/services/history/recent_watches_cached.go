package history

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"novastream/models"
)

// RecentWatchSnapshotItem keeps cap grouping private to the response builder.
// Group is a response-local ordinal, never a profile identifier or wire field.
type RecentWatchSnapshotItem struct {
	Group int                `json:"-"`
	Watch models.RecentWatch `json:"-"`
}

// RecentWatchesSnapshot owns its projection and remembers the exact history
// service inputs. It is neither an authorization grant nor a retained pool.
type RecentWatchesSnapshot struct {
	ObservedAt time.Time                 `json:"-"`
	Items      []RecentWatchSnapshotItem `json:"-"`
	owner      *Service
	revisions  map[string]watchStateRevision
}

type recentWatchesContributor struct {
	id        string
	name      string
	anonymous bool
}

type recentWatchesSnapshotRow struct {
	item   RecentWatchSnapshotItem
	rowKey string
}

func recentWatchesCanceled(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// SnapshotRecentWatches copies every eligible played watch before any language,
// visibility, per-profile cap or page selection. It performs no enrichment or
// mutation. The caller owns eligibleUsers for the duration of this call and must
// separately recheck contributor privacy and viewer authority before publishing.
func (s *Service) SnapshotRecentWatches(ctx context.Context, viewerID string, eligibleUsers map[string]models.User, windowDays int, observedAt time.Time) (*RecentWatchesSnapshot, error) {
	if s == nil || ctx == nil || viewerID == "" || strings.TrimSpace(viewerID) != viewerID || windowDays < 1 || windowDays > 90 || observedAt.IsZero() {
		return nil, errors.New("invalid recent watches snapshot request")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	done := ctx.Done()
	contributors := make([]recentWatchesContributor, 0, len(eligibleUsers))
	for id, user := range eligibleUsers {
		if recentWatchesCanceled(done) {
			return nil, ctx.Err()
		}
		if !user.SharesActivity() {
			continue
		}
		if id == "" || strings.TrimSpace(id) != id || user.ID != id {
			return nil, errors.New("invalid recent watches contributor identity")
		}
		anonymous := models.NormalizeActivityPrivacy(user.ActivityPrivacy) == models.ActivityPrivacySharedAnonymous
		name := user.Name
		if anonymous {
			name = "Fellow user"
		}
		contributors = append(contributors, recentWatchesContributor{id: id, name: name, anonymous: anonymous})
	}
	sort.Slice(contributors, func(i, j int) bool { return contributors[i].id < contributors[j].id })

	snapshot := &RecentWatchesSnapshot{
		ObservedAt: observedAt.UTC(),
		owner:      s,
		revisions:  make(map[string]watchStateRevision, len(contributors)+1),
	}
	cutoff := snapshot.ObservedAt.AddDate(0, 0, -windowDays)
	var rows []recentWatchesSnapshotRow
	// The cancellation channel is captured before locking. No service, metadata
	// or caller callback executes while history's read lock is held.
	collected := func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if recentWatchesCanceled(done) {
			return false
		}
		snapshot.revisions[viewerID] = s.watchStateRevisionLocked(viewerID)
		for group, contributor := range contributors {
			if recentWatchesCanceled(done) {
				return false
			}
			snapshot.revisions[contributor.id] = s.watchStateRevisionLocked(contributor.id)
			for key, item := range s.watchHistory[contributor.id] {
				if recentWatchesCanceled(done) {
					return false
				}
				if !item.Watched || !(item.WatchedSeconds > 0) || item.WatchedAt.Before(cutoff) {
					continue
				}
				var externalIDs map[string]string
				if item.ExternalIDs != nil {
					externalIDs = make(map[string]string, len(item.ExternalIDs))
					for provider, id := range item.ExternalIDs {
						externalIDs[provider] = id
					}
				}
				userID := contributor.id
				if contributor.anonymous {
					userID = ""
				}
				rows = append(rows, recentWatchesSnapshotRow{rowKey: key, item: RecentWatchSnapshotItem{
					Group: group + 1,
					Watch: models.RecentWatch{
						UserID: userID, UserName: contributor.name, IsAnonymous: contributor.anonymous,
						MediaType: item.MediaType, ItemID: item.ItemID, Name: item.Name,
						SeriesID: item.SeriesID, SeriesName: item.SeriesName,
						SeasonNumber: item.SeasonNumber, EpisodeNumber: item.EpisodeNumber,
						WatchedAt: item.WatchedAt, ExternalIDs: externalIDs,
					},
				}})
			}
		}
		return true
	}()
	if !collected {
		return nil, ctx.Err()
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i].item, rows[j].item
		if !a.Watch.WatchedAt.Equal(b.Watch.WatchedAt) {
			return a.Watch.WatchedAt.After(b.Watch.WatchedAt)
		}
		if a.Watch.MediaType != b.Watch.MediaType {
			return a.Watch.MediaType < b.Watch.MediaType
		}
		if a.Watch.ItemID != b.Watch.ItemID {
			return a.Watch.ItemID < b.Watch.ItemID
		}
		if a.Watch.SeriesID != b.Watch.SeriesID {
			return a.Watch.SeriesID < b.Watch.SeriesID
		}
		if a.Watch.SeasonNumber != b.Watch.SeasonNumber {
			return a.Watch.SeasonNumber < b.Watch.SeasonNumber
		}
		if a.Watch.EpisodeNumber != b.Watch.EpisodeNumber {
			return a.Watch.EpisodeNumber < b.Watch.EpisodeNumber
		}
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		return rows[i].rowKey < rows[j].rowKey
	})
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	snapshot.Items = make([]RecentWatchSnapshotItem, len(rows))
	for i := range rows {
		if recentWatchesCanceled(done) {
			return nil, ctx.Err()
		}
		snapshot.Items[i] = rows[i].item
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return snapshot, nil
}

// RecentWatchesSnapshotCurrent checks only the captured history inputs. Callers
// must still revalidate current users, privacy, filtering facts and permissions.
func (s *Service) RecentWatchesSnapshotCurrent(snapshot *RecentWatchesSnapshot) bool {
	if s == nil || snapshot == nil || snapshot.owner != s || len(snapshot.revisions) == 0 {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for id, revision := range snapshot.revisions {
		if s.watchStateRevisionLocked(id) != revision {
			return false
		}
	}
	return true
}
