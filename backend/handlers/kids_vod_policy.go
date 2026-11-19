package handlers

import (
	"context"
	"novastream/internal/datastore"
	"novastream/services/kidsvod"
)

// This current-state adapter is deliberately not installed on media routes.
// It reuses the existing core Policy and household applicability ports.
type kidsVODRegionalRepository interface {
	CurrentRegionalProfile(context.Context, string) (datastore.RegionalProfileSnapshot, error)
	CurrentRegionalAccount(context.Context, string) (datastore.RegionalAccountSnapshot, error)
}
type kidsVODRegionalPolicy struct{ current kidsVODRegionalRepository }

func (p *kidsVODRegionalPolicy) CurrentPolicy(ctx context.Context, id string) (kidsvod.Policy, error) {
	if p == nil || p.current == nil || ctx == nil || ctx.Err() != nil {
		return kidsvod.Policy{}, errKidsVODCurrentState
	}
	snapshot, err := p.current.CurrentRegionalProfile(ctx, id)
	if err != nil || snapshot.ProfileID != id || snapshot.AccountID == "" || snapshot.Revision == "" || ctx.Err() != nil {
		return kidsvod.Policy{}, errKidsVODCurrentState
	}
	if !snapshot.IsKids {
		return kidsvod.Policy{Enabled: false, Revision: snapshot.Revision}, nil
	}
	if snapshot.Policy == nil || !snapshot.Policy.Valid() {
		return kidsvod.Policy{}, errKidsVODCurrentState
	}
	movie, episode := snapshot.Policy.MaxMovieAge, snapshot.Policy.MaxEpisodeAge
	return kidsvod.Policy{Enabled: true, Country: snapshot.Policy.Country, MaxMovieAge: &movie, MaxEpisodeAge: &episode, Revision: snapshot.Revision}, nil
}
func (p *kidsVODRegionalPolicy) CurrentVODApplicability(ctx context.Context, id string) (vodAccountApplicability, error) {
	if p == nil || p.current == nil || ctx == nil || ctx.Err() != nil {
		return vodAccountApplicability{}, errVODOwner
	}
	snapshot, err := p.current.CurrentRegionalAccount(ctx, id)
	if err != nil || snapshot.AccountID != id || snapshot.Revision == "" || ctx.Err() != nil {
		return vodAccountApplicability{}, errVODOwner
	}
	mode := vodExplicitInactive
	if snapshot.HasChildren {
		mode = vodAccountActive
	}
	return vodAccountApplicability{mode: mode, revision: snapshot.Revision}, nil
}
