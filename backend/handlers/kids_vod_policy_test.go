package handlers

import (
	"context"
	"errors"
	"novastream/internal/datastore"
	"novastream/models"
	"testing"
)

type regionalPolicyRepositoryFixture struct {
	profile datastore.RegionalProfileSnapshot
	account datastore.RegionalAccountSnapshot
	err     error
}

func (f *regionalPolicyRepositoryFixture) CurrentRegionalProfile(context.Context, string) (datastore.RegionalProfileSnapshot, error) {
	return f.profile, f.err
}
func (f *regionalPolicyRepositoryFixture) CurrentRegionalAccount(context.Context, string) (datastore.RegionalAccountSnapshot, error) {
	return f.account, f.err
}
func TestCurrentRegionalAdapterNeverDisablesUnconfiguredChild(t *testing.T) {
	repository := &regionalPolicyRepositoryFixture{profile: datastore.RegionalProfileSnapshot{ProfileID: "child", AccountID: "household", Revision: "current-v1", IsKids: true}, account: datastore.RegionalAccountSnapshot{AccountID: "household", Revision: "membership-v1", HasChildren: true}}
	reader := &kidsVODRegionalPolicy{current: repository}
	if _, err := reader.CurrentPolicy(context.Background(), "child"); err == nil {
		t.Fatal("missing child policy became disabled")
	}
	state, err := reader.CurrentVODApplicability(context.Background(), "household")
	if err != nil || state.mode != vodAccountActive {
		t.Fatal("unconfigured child disabled family applicability")
	}
	repository.profile.Policy = &models.RegionalChildPolicy{Version: 1, Country: "DE", MaxMovieAge: 0, MaxEpisodeAge: 18}
	policy, err := reader.CurrentPolicy(context.Background(), "child")
	if err != nil || !policy.Enabled || *policy.MaxMovieAge != 0 || *policy.MaxEpisodeAge != 18 {
		t.Fatal("current distinct caps not mapped")
	}
	*policy.MaxMovieAge = 18
	if repository.profile.Policy.MaxMovieAge != 0 {
		t.Fatal("caller changed repository configuration")
	}
}
func TestCurrentRegionalAdapterDistinguishesHealthyAdultFromUnavailableState(t *testing.T) {
	repository := &regionalPolicyRepositoryFixture{profile: datastore.RegionalProfileSnapshot{ProfileID: "adult", AccountID: "household", Revision: "profile-v1"}, account: datastore.RegionalAccountSnapshot{AccountID: "household", Revision: "account-v1"}}
	reader := &kidsVODRegionalPolicy{current: repository}
	policy, err := reader.CurrentPolicy(context.Background(), "adult")
	if err != nil || policy.Enabled || policy.Revision == "" {
		t.Fatal("proven adult compatibility lost")
	}
	state, err := reader.CurrentVODApplicability(context.Background(), "household")
	if err != nil || state.mode != vodExplicitInactive {
		t.Fatal("known adult account did not remain inactive")
	}
	repository.err = errors.New("synthetic current read failure")
	if _, err = reader.CurrentPolicy(context.Background(), "adult"); err == nil {
		t.Fatal("failed profile read became adult")
	}
	if _, err = reader.CurrentVODApplicability(context.Background(), "household"); err == nil {
		t.Fatal("failed account read became inactive")
	}
}
