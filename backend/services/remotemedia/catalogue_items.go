package remotemedia

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"novastream/models"
	"novastream/services/localmedia"
)

// ErrInvalidQuery identifies an unsupported mirrored-catalogue item sort.
var ErrInvalidQuery = errors.New("invalid remote catalogue query")

// ListItems pages stored provider versions, rather than a page of title groups.
// Mirrored metadata describes the catalogue; it does not authorize a source or
// establish the identity or language of the bytes behind a provider item.
func (s *Service) ListItems(ctx context.Context, libraryID string, query models.LocalMediaItemListQuery) (*models.LocalMediaItemListResult, error) {
	if ctx == nil {
		return nil, errors.New("remote catalogue context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := localmedia.ValidateMediaFilters(query.MediaFilters); err != nil {
		return nil, err
	}
	mode := strings.TrimSpace(query.Sort)
	switch mode {
	case "", "name", "created", "updated", "year", "size":
	default:
		return nil, fmt.Errorf("%w: unsupported sort", ErrInvalidQuery)
	}
	if s == nil || s.repo == nil {
		return nil, errors.New("remote catalogue unavailable")
	}
	library, err := s.repo.GetLibrary(ctx, libraryID)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if library == nil {
		return nil, ErrNotFound
	}
	rows, err := s.repo.ListItems(ctx, libraryID, query.IncludeMissing)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	filter := strings.TrimSpace(query.Filter)
	include := filter == "" || filter == "all" || filter == string(models.LocalMediaMatchStatusMatched) || filter == string(models.LocalMediaMatchStatusManual)
	term := strings.ToLower(strings.TrimSpace(query.Query))
	selected := make([]catalogueItemRow, 0, len(rows))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !localmedia.MatchesMediaFilters(remoteFilterProbe(row), query.MediaFilters) {
			continue
		}
		if !include {
			continue
		}
		title := strings.ToLower(row.Title)
		if term != "" && !strings.Contains(title, term) && !strings.Contains(strings.ToLower(row.FileName), term) {
			continue
		}
		selected = append(selected, catalogueItemRow{item: row, title: title})
	}
	// Merge operations check the original cancellation without making the
	// comparator depend on a changing context or doing additional repository I/O.
	if err := sortCatalogueItems(ctx, selected, mode, mode != "" && query.Dir == "desc"); err != nil {
		return nil, err
	}
	total := len(selected)
	limit := query.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}
	if offset > total {
		offset = total
	}
	count := limit
	if count > total-offset {
		count = total - offset
	}
	result := &models.LocalMediaItemListResult{Items: make([]models.LocalMediaItem, 0, count), Total: total, Limit: limit, Offset: offset}
	for _, row := range selected[offset : offset+count] {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		item := toLocalItem(library, row.item)
		item.CreatedAt, item.UpdatedAt, item.IsMissing = row.item.CreatedAt, row.item.UpdatedAt, row.item.IsMissing
		result.Items = append(result.Items, item)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

type catalogueItemRow struct {
	item  models.RemoteMediaItem
	title string
}

func sortCatalogueItems(ctx context.Context, rows []catalogueItemRow, mode string, desc bool) error {
	scratch := make([]catalogueItemRow, len(rows))
	var merge func(int, int) error
	merge = func(lo, hi int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if hi-lo < 2 {
			return nil
		}
		mid := lo + (hi-lo)/2
		if err := merge(lo, mid); err != nil {
			return err
		}
		if err := merge(mid, hi); err != nil {
			return err
		}
		left, right := lo, mid
		for out := lo; out < hi; out++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if left < mid && (right == hi || catalogueItemLess(rows[left], rows[right], mode, desc)) {
				scratch[out] = rows[left]
				left++
			} else {
				scratch[out] = rows[right]
				right++
			}
		}
		copy(rows[lo:hi], scratch[lo:hi])
		return ctx.Err()
	}
	return merge(0, len(rows))
}

func catalogueItemLess(a, b catalogueItemRow, mode string, desc bool) bool {
	cmp := 0
	switch mode {
	case "created":
		cmp = a.item.CreatedAt.Compare(b.item.CreatedAt)
	case "updated":
		cmp = a.item.UpdatedAt.Compare(b.item.UpdatedAt)
	case "year":
		if a.item.Year < b.item.Year {
			cmp = -1
		} else if a.item.Year > b.item.Year {
			cmp = 1
		}
	case "size":
		if a.item.SizeBytes < b.item.SizeBytes {
			cmp = -1
		} else if a.item.SizeBytes > b.item.SizeBytes {
			cmp = 1
		}
	default:
		cmp = strings.Compare(a.title, b.title)
	}
	if cmp != 0 {
		if desc {
			return cmp > 0
		}
		return cmp < 0
	}
	if a.title != b.title {
		return a.title < b.title
	}
	return a.item.ID < b.item.ID
}
