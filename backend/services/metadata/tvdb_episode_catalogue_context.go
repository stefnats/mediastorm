package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	episodeCatalogueOrigin      = "https://api4.thetvdb.com"
	episodeCatalogueMaxPages    = 256
	episodeCataloguePageBytes   = 1 << 20
	episodeCatalogueTotalBytes  = 16 << 20
	episodeCatalogueMaxEpisodes = 20000
)

// The mutex may also be held by an older metadata request. Waiting for it must
// consume this original operation's budget rather than create a new lifetime.
func lockEpisodeCatalogue(ctx context.Context, mu *sync.Mutex) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if mu.TryLock() {
			if ctx.Err() != nil {
				mu.Unlock()
				return ctx.Err()
			}
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *tvdbClient) catalogueHTTP(ctx context.Context, method, endpoint string, body []byte, token string, limit int64) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := *c.httpc
	originalRedirect := client.CheckRedirect
	client.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if len(via) >= 10 || len(via) == 0 || next.URL.Scheme != "https" || next.URL.Host != "api4.thetvdb.com" || next.URL.User != nil || next.URL.Fragment != "" || next.URL.String() != via[0].URL.String() {
			return errEpisodeDateCatalogueUnavailable
		}
		if originalRedirect != nil {
			return originalRedirect(next, via)
		}
		return nil
	}
	response, err := client.Do(req)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, err
	}
	if response.Body == nil {
		return nil, errEpisodeDateCatalogueUnavailable
	}
	// Close is part of the charged producer stack. A late body/Close can never
	// publish an observation, nor release its slot merely on cancellation.
	data, readErr := io.ReadAll(io.LimitReader(response.Body, limit+1))
	closeErr := response.Body.Close()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if response.StatusCode != http.StatusOK || int64(len(data)) > limit {
		return nil, errEpisodeDateCatalogueUnavailable
	}
	return data, nil
}

func decodeEpisodeCatalogue(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errEpisodeDateCatalogueUnavailable
	}
	return nil
}

func (c *tvdbClient) catalogueToken(ctx context.Context) (string, error) {
	if c == nil || !c.isConfigured() || len(c.apiKey) > 4096 || c.httpc == nil {
		return "", errEpisodeDateCatalogueUnavailable
	}
	if err := lockEpisodeCatalogue(ctx, &c.mu); err != nil {
		return "", err
	}
	defer c.mu.Unlock()
	if c.token != "" && len(c.token) <= 16384 && time.Now().Before(c.tokenExpiry.Add(-time.Minute)) {
		return c.token, nil
	}
	body, err := json.Marshal(map[string]string{"apikey": c.apiKey})
	if err != nil {
		return "", err
	}
	data, err := c.catalogueHTTP(ctx, http.MethodPost, episodeCatalogueOrigin+"/v4/login", body, "", 65536)
	if err != nil {
		return "", err
	}
	var response struct {
		Status string `json:"status"`
		Data   struct {
			Token string `json:"token"`
		} `json:"data"`
	}
	if err = decodeEpisodeCatalogue(data, &response); err != nil {
		return "", err
	}
	if response.Status != "success" || response.Data.Token == "" || len(response.Data.Token) > 16384 || ctx.Err() != nil {
		return "", errEpisodeDateCatalogueUnavailable
	}
	c.token = response.Data.Token
	c.tokenExpiry = time.Now().Add(24 * time.Hour)
	return c.token, nil
}

func (c *tvdbClient) catalogueThrottle(ctx context.Context) error {
	if err := lockEpisodeCatalogue(ctx, &c.throttleMu); err != nil {
		return err
	}
	defer c.throttleMu.Unlock()
	wait := time.Until(c.lastRequest.Add(c.minInterval))
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.lastRequest = time.Now()
	return nil
}

func nextEpisodeCataloguePage(raw json.RawMessage, endpoint string, page int) (bool, error) {
	if len(raw) == 0 {
		return false, errEpisodeDateCatalogueUnavailable
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false, nil
	}
	var supplied string
	if err := json.Unmarshal(raw, &supplied); err != nil || supplied == "" {
		return false, errEpisodeDateCatalogueUnavailable
	}
	next, err := url.Parse(supplied)
	if err != nil {
		return false, errEpisodeDateCatalogueUnavailable
	}
	origin, _ := url.Parse(episodeCatalogueOrigin + "/")
	next = origin.ResolveReference(next)
	expected, _ := url.Parse(endpoint)
	query, err := url.ParseQuery(next.RawQuery)
	if err != nil || next.Scheme != "https" || next.Host != "api4.thetvdb.com" || next.User != nil || next.Fragment != "" || next.EscapedPath() != expected.EscapedPath() || len(query) != 1 || len(query["page"]) != 1 || query.Get("page") != strconv.Itoa(page+1) {
		return false, errEpisodeDateCatalogueUnavailable
	}
	return true, nil
}

func (c *tvdbClient) completeEpisodeCatalogue(work *episodeDateWork, id int64, ordering, language string) ([]tvdbEpisode, *episodeCatalogueObservation, error) {
	ctx := work.ctx
	observed := time.Now()
	if ctx.Err() != nil || id <= 0 || len(ordering) > 32 || ordering == "" || strings.IndexFunc(ordering, func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') }) >= 0 {
		return nil, nil, errEpisodeDateCatalogueUnavailable
	}
	language = normalizeTVDBLanguage(language)
	if len(language) != 3 || strings.IndexFunc(language, func(r rune) bool { return r < 'a' || r > 'z' }) >= 0 {
		return nil, nil, errEpisodeDateCatalogueUnavailable
	}
	endpoint := fmt.Sprintf("%s/v4/series/%d/episodes/%s/%s", episodeCatalogueOrigin, id, ordering, language)
	token, err := c.catalogueToken(ctx)
	if err != nil {
		return nil, nil, err
	}
	var rows []tvdbEpisode
	totalBytes := 0
	for page := 0; page < episodeCatalogueMaxPages; page++ {
		if err = c.catalogueThrottle(ctx); err != nil {
			return nil, nil, err
		}
		// Supplied Next is validation DATA only. The request URL is always built
		// from the original official origin/path/language and exact next index.
		data, requestErr := c.catalogueHTTP(ctx, http.MethodGet, endpoint+"?page="+strconv.Itoa(page), nil, token, episodeCataloguePageBytes)
		if requestErr != nil {
			return nil, nil, requestErr
		}
		totalBytes += len(data)
		if totalBytes > episodeCatalogueTotalBytes {
			return nil, nil, errEpisodeDateCatalogueUnavailable
		}
		var response struct {
			Status string `json:"status"`
			Data   struct {
				Episodes []tvdbEpisode `json:"episodes"`
			} `json:"data"`
			Links struct {
				Next json.RawMessage `json:"next"`
			} `json:"links"`
		}
		if err = decodeEpisodeCatalogue(data, &response); err != nil {
			return nil, nil, err
		}
		if response.Status != "success" || response.Data.Episodes == nil || len(rows)+len(response.Data.Episodes) > episodeCatalogueMaxEpisodes {
			return nil, nil, errEpisodeDateCatalogueUnavailable
		}
		rows = append(rows, response.Data.Episodes...)
		more, nextErr := nextEpisodeCataloguePage(response.Links.Next, endpoint, page)
		if nextErr != nil {
			return nil, nil, nextErr
		}
		if !more {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			return rows, &episodeCatalogueObservation{work: work, series: id, ordering: ordering, language: language, rows: rows, observed: observed}, nil
		}
	}
	return nil, nil, errEpisodeDateCatalogueUnavailable
}
