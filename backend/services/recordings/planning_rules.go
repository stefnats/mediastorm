package recordings

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"time"

	"novastream/models"
)

const (
	planningRuleLimit        = 200
	planningRuleChannelLimit = 500
	planningRuleProgramLimit = 20000
	planningRuleMatchLimit   = 2000
)

type ruleUncertainty struct {
	key        string // Empty means the affected provider could not be established.
	start, end time.Time
}

type ruleProjection struct {
	jobs        []models.Recording
	metadata    *models.RecordingRuleForecast
	uncertainty []ruleUncertainty
	partial     bool
	knownGuide  bool
}

func (p *ruleProjection) uncertain(window planningWindow) bool {
	for _, gap := range p.uncertainty {
		if (gap.key == "" || gap.key == window.capacity.Key) && gap.start.Before(window.end) && gap.end.After(window.start) {
			return true
		}
	}
	return false
}

func (p *ruleProjection) gap(key string, start, end time.Time) {
	if !end.After(start) {
		return
	}
	p.partial = true
	p.uncertainty = append(p.uncertainty, ruleUncertainty{key, start, end})
}

func ruleHistoryKey(ruleID, scheduleKey string) string { return ruleID + "\x00" + scheduleKey }

func recordingScheduleKey(job models.Recording) string {
	if job.ScheduleKey != "" {
		return job.ScheduleKey
	}
	if strings.TrimSpace(job.TvgID) != "" && !job.StartAt.IsZero() {
		return strings.ToLower(strings.TrimSpace(job.TvgID)) + ":" + job.StartAt.UTC().Format(time.RFC3339Nano)
	}
	return ""
}

// projectRecordingRules reads only repositories, the current channel catalog and
// the local EPG cache. It never calls expansion or creates a persisted job.
func (s *Service) projectRecordingRules(ctx context.Context, profileID string, now time.Time, jobs []models.Recording, resolve func(models.Recording) (RecordingCapacity, bool)) ruleProjection {
	p := ruleProjection{jobs: jobs}
	if s.rules == nil {
		return p
	}
	end := now.Add(7 * 24 * time.Hour)
	unknownEnd := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	p.metadata = &models.RecordingRuleForecast{Status: "not_configured", HorizonStart: now, HorizonEnd: end}
	rules, err := s.rules.ListRules(ctx, "", true, true)
	if err != nil {
		p.metadata.Status = "unavailable"
		p.gap("", now, unknownEnd)
		return p
	}
	enabled := make([]models.RecordingRule, 0, len(rules))
	for _, rule := range rules {
		if rule.Enabled {
			enabled = append(enabled, rule)
		}
	}
	if len(enabled) == 0 {
		return p
	}
	sort.Slice(enabled, func(i, j int) bool { return enabled[i].ID < enabled[j].ID })
	if len(enabled) > planningRuleLimit {
		enabled = enabled[:planningRuleLimit]
		p.gap("", now, unknownEnd)
	}
	// The cache horizon is finite even when all channels have complete guide
	// entries inside it. A later requested recording is still uncertain.
	p.uncertainty = append(p.uncertainty, ruleUncertainty{"", end, unknownEnd})
	if s.epg == nil || s.channels == nil || ctx.Err() != nil {
		p.metadata.Status = "unavailable"
		p.gap("", now, unknownEnd)
		return p
	}
	// A second all-status snapshot prevents cancelled/completed airings from
	// being projected again. Merge new active rows: expansion can race the
	// initial active read, but deduplication must not make its demand disappear.
	history, err := s.repo.List(ctx, models.RecordingListFilter{IncludeAll: true, Limit: planningJobLimit + 1, OnlyStartBefore: &end})
	if err != nil || len(history) > planningJobLimit {
		p.metadata.Status = "unavailable"
		p.gap("", now, unknownEnd)
		return p
	}
	byID := make(map[string]int, len(jobs))
	for i, job := range p.jobs {
		if job.ID != "" {
			byID[job.ID] = i
		}
	}
	historyByAiring := make(map[string]models.Recording)
	invalidHistoryRules := make(map[string]bool)
	for _, job := range history {
		if i, exists := byID[job.ID]; exists && job.ID != "" {
			p.jobs[i] = job
		} else if job.ID != "" && planningStatus(job.Status) {
			if len(byID) >= planningJobLimit {
				p.gap("", now, unknownEnd)
			} else {
				byID[job.ID] = len(p.jobs)
				p.jobs = append(p.jobs, job)
			}
		}
		if job.RuleID != "" {
			if key := recordingScheduleKey(job); key != "" {
				historyByAiring[ruleHistoryKey(job.RuleID, key)] = job
			} else {
				invalidHistoryRules[job.RuleID] = true
			}
		}
	}
	type profileGuide struct {
		channels []Channel
		schedule map[string][]models.EPGProgram
		offset   time.Duration
		ok       bool
	}
	guides := make(map[string]profileGuide)
	projected := make(map[string]models.Recording)
	processedPrograms := 0
	for _, rule := range enabled {
		if ctx.Err() != nil {
			p.gap("", now, unknownEnd)
			break
		}
		if rule.ID == "" || rule.UserID == "" || invalidHistoryRules[rule.ID] || rule.PaddingBeforeSeconds < 0 || rule.PaddingBeforeSeconds > 3600 || rule.PaddingAfterSeconds < 0 || rule.PaddingAfterSeconds > 3600 {
			p.gap("", now, unknownEnd)
			continue
		}
		var compiled *regexp.Regexp
		if len(rule.Pattern) > 4096 || rule.MatchType != models.RecordingRuleMatchTitle && rule.MatchType != models.RecordingRuleMatchRegex {
			p.gap("", now, unknownEnd)
			continue
		}
		if rule.MatchType == models.RecordingRuleMatchRegex {
			compiled, err = compileRecordingRegex(rule.Pattern)
			if err != nil {
				p.gap("", now, unknownEnd)
				continue
			}
		}
		guide, exists := guides[rule.UserID]
		if !exists {
			channels, channelErr := s.channels.RecordingChannels(ctx, rule.UserID)
			guide.ok = channelErr == nil && len(channels) <= planningRuleChannelLimit && ctx.Err() == nil
			if s.epgoffset != nil {
				guide.offset = s.epgoffset(rule.UserID)
			}
			if guide.offset < -48*time.Hour || guide.offset > 48*time.Hour {
				guide.ok = false
			}
			if guide.ok {
				// Match the scheduler's last-catalog-entry behavior for a TVG ID.
				byTVG := make(map[string]Channel)
				for _, channel := range channels {
					if id := strings.TrimSpace(channel.TvgID); id != "" {
						byTVG[strings.ToLower(id)] = channel
					}
				}
				ids := make([]string, 0, len(byTVG))
				for id := range byTVG {
					ids = append(ids, id)
				}
				sort.Strings(ids)
				for _, id := range ids {
					guide.channels = append(guide.channels, byTVG[id])
				}
				guide.schedule = s.epg.GetScheduleMultiple(ids, now.Add(-guide.offset), end.Add(-guide.offset))
			}
			guides[rule.UserID] = guide
		}
		if !guide.ok {
			p.gap("", now, unknownEnd)
			continue
		}
		matchedChannel := false
		for _, channel := range guide.channels {
			if !ruleMatchesChannel(rule, channel) {
				continue
			}
			matchedChannel = true
			if ctx.Err() != nil || processedPrograms >= planningRuleProgramLimit || len(projected) >= planningRuleMatchLimit {
				p.gap("", now, unknownEnd)
				break
			}
			capacity, routed := resolve(models.Recording{UserID: rule.UserID, ChannelID: channel.ID, TvgID: channel.TvgID, SourceURL: channel.SourceURL})
			if !routed || channel.SourceURL == "" {
				p.gap("", now, unknownEnd)
				continue
			}
			programs := guide.schedule[strings.ToLower(strings.TrimSpace(channel.TvgID))]
			if len(programs) > planningRuleProgramLimit-processedPrograms {
				processedPrograms = planningRuleProgramLimit
				p.gap(capacity.Key, now, unknownEnd)
				continue
			}
			processedPrograms += len(programs)
			coverage := make([]ruleUncertainty, 0, len(programs))
			guideFlagsNew := rule.NewOnly && guideMarksNewAirings(programs)
			for _, program := range programs {
				if ctx.Err() != nil {
					p.gap("", now, unknownEnd)
					break
				}
				if program.Start.IsZero() || !program.Stop.After(program.Start) {
					p.gap(capacity.Key, now, unknownEnd)
					continue
				}
				program.Start = program.Start.Add(guide.offset)
				program.Stop = program.Stop.Add(guide.offset)
				if program.Start.Year() < 1 || program.Stop.Year() > 9999 {
					p.gap(capacity.Key, now, unknownEnd)
					continue
				}
				if program.Stop.After(now) && program.Start.Before(end) {
					coverage = append(coverage, ruleUncertainty{start: program.Start, end: program.Stop})
					p.knownGuide = true
				}
				if !program.Stop.After(now) || !program.Start.Before(end) || !matchesRecordingRule(rule, compiled, program.Title) || rule.NewOnly && !isNewAiring(program, guideFlagsNew) {
					continue
				}
				key := strings.ToLower(strings.TrimSpace(channel.TvgID)) + ":" + program.Start.UTC().Format(time.RFC3339Nano)
				airedKey := ruleHistoryKey(rule.ID, key)
				if prior, exists := historyByAiring[airedKey]; exists && !(prior.Status == models.RecordingStatusCancelled && prior.Error == "recording rule paused") {
					continue
				}
				if len(projected) >= planningRuleMatchLimit {
					p.gap("", now, unknownEnd)
					break
				}
				projected[airedKey] = models.Recording{UserID: rule.UserID, RuleID: rule.ID, ScheduleKey: key, Type: models.RecordingTypeEPG,
					Status: models.RecordingStatusPending, ChannelID: channel.ID, TvgID: channel.TvgID, ChannelName: channel.Name,
					Title: program.Title, SourceURL: channel.SourceURL, StartAt: program.Start.UTC(), EndAt: program.Stop.UTC(),
					PaddingBeforeSeconds: rule.PaddingBeforeSeconds, PaddingAfterSeconds: rule.PaddingAfterSeconds}
			}
			// Any uncovered guide segment could contain another matching airing.
			// Expand that uncertainty by this rule's recording buffers.
			sort.Slice(coverage, func(i, j int) bool { return coverage[i].start.Before(coverage[j].start) })
			cursor := now
			for _, interval := range coverage {
				if interval.start.After(cursor) {
					gapEnd := interval.start
					if gapEnd.After(end) {
						gapEnd = end
					}
					p.gap(capacity.Key, cursor.Add(-time.Duration(rule.PaddingBeforeSeconds)*time.Second), gapEnd.Add(time.Duration(rule.PaddingAfterSeconds)*time.Second))
				}
				if interval.end.After(cursor) {
					cursor = interval.end
				}
				if !cursor.Before(end) {
					break
				}
			}
			if cursor.Before(end) {
				p.gap(capacity.Key, cursor.Add(-time.Duration(rule.PaddingBeforeSeconds)*time.Second), end.Add(time.Duration(rule.PaddingAfterSeconds)*time.Second))
			}
		}
		if !matchedChannel {
			p.gap("", now, unknownEnd)
		}
	}
	keys := make([]string, 0, len(projected))
	for key := range projected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		job := projected[key]
		p.jobs = append(p.jobs, job)
		if job.UserID == profileID {
			p.metadata.ProjectedCount++
		}
	}
	p.metadata.Status = "available"
	if p.partial {
		p.metadata.Status = "partial"
	}
	if !p.knownGuide {
		p.metadata.Status = "unavailable"
	}
	return p
}
