/*
 * ● AnvuMusic
 * ○ A high-performance engine for streaming music in Telegram voicechats.
 *
 * Copyright (C) 2026 Team Echo
 *
 * Configured YouTube Stream/Download Providers:
 *   1. Yuki       — direct audio stream API credit goes to @Z0iiw
 *   2. ShrutiApi  — direct audio stream (Primary: api.shrutibots.site, Backup: api01.shrutibots.site) credit goes to @Yaduwanshi_Nand
 */

package platforms

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Laky-64/gologging"
	"github.com/amarnathcjd/gogram/telegram"

	"main/internal/config"
	state "main/internal/core/models"
)

const (
	PlatformShrutiApi    state.PlatformName = "ShrutiApi"
	yukiDefaultBaseURL                      = "https://music.yukiapi.site"
	shrutiPrimaryBaseURL                    = "https://api.shrutibots.in"
	shrutiBackupBaseURL                     = "https://api01.shrutibots.in"
)

var (
	shrutiUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
	shrutiAccept    = "application/json, text/plain, */*"

	saveMu         sync.Mutex
	healthMu       sync.Mutex
	providerHealth = make(map[string]providerHealthState)
)

type providerHealthState struct {
	failures int
	opened   time.Time
}

func providerAllowed(name string) bool {
	healthMu.Lock()
	defer healthMu.Unlock()
	h := providerHealth[name]
	return h.opened.IsZero() || time.Since(h.opened) >= time.Minute
}

func providerResult(name string, err error) {
	healthMu.Lock()
	defer healthMu.Unlock()
	h := providerHealth[name]
	if err == nil {
		delete(providerHealth, name)
		return
	}
	h.failures++
	if h.failures >= 3 {
		h.opened = time.Now()
		h.failures = 0
	}
	providerHealth[name] = h
}

type ShrutiApiPlatform struct {
	name   state.PlatformName
	client *http.Client
}

// NOTE: getYukiKeys() / getShrutiKeys() ab keypool.go me hain
// (comma separated multiple keys support: "key1,key2").

func getYukiURL() string {
	if config.YukiAPIURL != "" {
		return strings.TrimRight(config.YukiAPIURL, "/")
	}
	if u := strings.TrimSpace(os.Getenv("YUKI_API_URL")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return yukiDefaultBaseURL
}

func getShrutiPrimaryURL() string {
	if config.ShrutiAPIURL != "" {
		return strings.TrimRight(config.ShrutiAPIURL, "/")
	}
	if u := strings.TrimSpace(os.Getenv("SHRUTI_API_URL")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return shrutiPrimaryBaseURL
}

func getShrutiBackupURL() string {
	if config.ShrutiBackupURL != "" {
		return strings.TrimRight(config.ShrutiBackupURL, "/")
	}
	if u := strings.TrimSpace(os.Getenv("SHRUTI_BACKUP_URL")); u != "" {
		return strings.TrimRight(u, "/")
	}
	return shrutiBackupBaseURL
}

func init() {
	Register(85, &ShrutiApiPlatform{
		name:   PlatformShrutiApi,
		client: &http.Client{Timeout: 90 * time.Second},
	})
}

func (s *ShrutiApiPlatform) Name() state.PlatformName { return s.name }

func (s *ShrutiApiPlatform) CanGetTracks(_ string) bool { return false }

func (s *ShrutiApiPlatform) GetTracks(_ string, _ bool) ([]*state.Track, error) {
	return nil, errors.New("shrutiapi is a download-only platform")
}

func (s *ShrutiApiPlatform) CanDownload(source state.PlatformName) bool {
	return source == PlatformYouTube
}

func (s *ShrutiApiPlatform) CanSearch() bool { return false }

func (s *ShrutiApiPlatform) Search(_ string, _ bool) ([]*state.Track, error) {
	return nil, errors.New("shrutiapi does not support search")
}

// Download races all configured API providers (Yuki and Shruti). The first provider to produce a
// non-empty file wins; canceling the shared context stops the other requests.
// Har provider ke andar multiple API keys rotate hoti hain (KeyPool).
func (s *ShrutiApiPlatform) Download(
	ctx context.Context,
	track *state.Track,
	statusMsg *telegram.NewMessage,
) (string, error) {
	if f := findFile(track); f != "" {
		gologging.Debug("ShrutiApi: cache hit -> " + f)
		return f, nil
	}

	mediaType := "audio"
	ext := ".webm"
	if track.Video {
		mediaType = "video"
		ext = ".mkv"
	}

	youtubeURL := "https://www.youtube.com/watch?v=" + track.ID
	encodedURL := url.QueryEscape(youtubeURL)

	type apiAttempt struct {
		name string
		fn   func(context.Context) (string, error)
	}

	var attempts []apiAttempt

	// 1. Yuki — key rotation (har key 100, 2 keys = total 200)
	if yukiPool().Available() {
		attempts = append(attempts, apiAttempt{"Yuki", func(c context.Context) (string, error) {
			return yukiPool().Do(func(key string) (string, error) {
				return s.downloadWithYuki(c, track.ID, mediaType, track, ext, key)
			})
		}})
	}

	// 2. ShrutiApi — key rotation. Primary + Backup ek hi key share karte hain,
	//    isliye ek download = 1 count.
	if shrutiPool().Available() {
		primaryURL := getShrutiPrimaryURL()
		backupURL := getShrutiBackupURL()
		attempts = append(attempts, apiAttempt{"Shruti", func(c context.Context) (string, error) {
			return shrutiPool().Do(func(key string) (string, error) {
				return s.shrutiRace(c, []string{primaryURL, backupURL}, encodedURL, mediaType, track, ext, key)
			})
		}})
	}

	if len(attempts) == 0 {
		return "", errors.New("shrutiapi: no API key available (not configured or all keys exhausted)")
	}

	const apiTimeout = 15 * time.Second
	traceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		name, path string
		err        error
	}
	results := make(chan result, len(attempts))

	for _, attempt := range attempts {
		a := attempt
		go func() {
			if !providerAllowed(a.name) {
				results <- result{name: a.name, err: errors.New("circuit open")}
				return
			}
			timeoutCtx, timeoutCancel := context.WithTimeout(traceCtx, apiTimeout)
			defer timeoutCancel()
			path, err := a.fn(timeoutCtx)
			if err == nil && !fileExists(path) {
				err = errors.New("provider returned an empty file")
			}
			providerResult(a.name, err)
			results <- result{a.name, path, err}
		}()
	}

	var errs []string
	for range attempts {
		r := <-results
		if r.err == nil {
			cancel()
			gologging.InfoF("ShrutiApi: downloaded %s via %s -> %s", track.ID, r.name, r.path)
			return r.path, nil
		}
		if !errors.Is(r.err, context.Canceled) && !errors.Is(r.err, context.DeadlineExceeded) {
			gologging.WarnF("ShrutiApi [%s] failed for %s: %v", r.name, track.ID, r.err)
			errs = append(errs, r.name+": "+r.err.Error())
		}
	}
	if len(errs) > 0 {
		return "", fmt.Errorf("shrutiapi: all APIs failed:\n  • %s", strings.Join(errs, "\n  • "))
	}
	return "", ctx.Err()
}

// ── Yuki API ──────────────────────────────────────────────────
// Endpoint: GET {yukiBaseURL}/stream/{video_id}?key={key}
// Direct binary audio stream.

func (s *ShrutiApiPlatform) downloadWithYuki(
	ctx context.Context,
	videoID, mediaType string,
	track *state.Track,
	ext string,
	apiKey string,
) (string, error) {
	baseURL := getYukiURL()
	endpoint := fmt.Sprintf("%s/stream/%s?key=%s", baseURL, videoID, url.QueryEscape(apiKey))

	// key ko log me nahi dikhate (security)
	gologging.DebugF("ShrutiApi: Yuki requesting for %s via %s", track.ID, baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("build yuki request: %w", err)
	}
	req.Header.Set("User-Agent", shrutiUserAgent)

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("yuki request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("yuki HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return s.saveToFile(resp.Body, track, ext)
}

// ── Shruti API ──────────────────────────────────────────────
// Primary: https://api.shrutibots.in
// Backup:  https://api01.shrutibots.in
// Endpoint: GET /download?url={encoded_yt_url}&type={media_type}&api_key={api_key}

// shrutiRace runs primary & backup in parallel with the SAME key; first success wins.
func (s *ShrutiApiPlatform) shrutiRace(
	ctx context.Context,
	baseURLs []string,
	encodedURL, mediaType string,
	track *state.Track,
	ext, key string,
) (string, error) {
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type res struct {
		path string
		err  error
	}
	ch := make(chan res, len(baseURLs))
	for _, b := range baseURLs {
		base := b
		go func() {
			p, err := s.downloadWithShruti(rctx, base, encodedURL, mediaType, track, ext, key)
			ch <- res{p, err}
		}()
	}

	var lastErr, quotaErr error
	for range baseURLs {
		r := <-ch
		if r.err == nil {
			return r.path, nil
		}
		if isQuotaErr(r.err) {
			quotaErr = r.err
		}
		lastErr = r.err
	}
	if quotaErr != nil {
		return "", quotaErr
	}
	return "", lastErr
}

func (s *ShrutiApiPlatform) downloadWithShruti(
	ctx context.Context,
	baseURL, encodedURL, mediaType string,
	track *state.Track,
	ext string,
	apiKey string,
) (string, error) {
	endpoint := fmt.Sprintf(
		"%s/download?url=%s&type=%s&api_key=%s",
		baseURL, encodedURL, mediaType, url.QueryEscape(apiKey),
	)

	gologging.DebugF("ShrutiApi: requesting stream for %s (%s) via %s", track.ID, mediaType, baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("User-Agent", shrutiUserAgent)
	req.Header.Set("Accept", shrutiAccept)

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("shruti HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read shruti response: %w", err)
	}

	// Direct binary audio/video stream response
	if !strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "application/json") {
		return s.saveToFile(bytes.NewReader(body), track, ext)
	}

	// JSON response with download_token
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		return "", fmt.Errorf("decode shruti token response: %w", err)
	}

	if status, _ := data["status"].(string); status != "success" {
		// server ka message bhi error me daal rahe hain taaki quota/limit detect ho sake
		msg := ""
		if m, ok := data["message"].(string); ok {
			msg = m
		} else if m, ok := data["error"].(string); ok {
			msg = m
		}
		return "", fmt.Errorf("status=%s %s", status, msg)
	}

	token, _ := data["download_token"].(string)
	if token == "" {
		return "", errors.New("no download_token in response")
	}

	streamURL := fmt.Sprintf("%s/stream/%s?token=%s&type=%s", baseURL, track.ID, token, mediaType)
	gologging.DebugF("ShrutiApi: streaming from %s", streamURL)

	sreq, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		return "", fmt.Errorf("build stream request: %w", err)
	}
	sreq.Header.Set("User-Agent", shrutiUserAgent)
	sreq.Header.Set("Accept", shrutiAccept)

	sresp, err := s.client.Do(sreq)
	if err != nil {
		return "", fmt.Errorf("stream request failed: %w", err)
	}
	defer sresp.Body.Close()

	if sresp.StatusCode != http.StatusOK {
		sbody, _ := io.ReadAll(sresp.Body)
		return "", fmt.Errorf("stream HTTP %d: %s", sresp.StatusCode, strings.TrimSpace(string(sbody)))
	}

	return s.saveToFile(sresp.Body, track, ext)
}

// ── Helpers ─────────────────────────────────────────────────

func (s *ShrutiApiPlatform) saveToFile(
	reader io.Reader,
	track *state.Track,
	ext string,
) (string, error) {
	path := getPath(track, ext)
	if err := os.MkdirAll("downloads", 0o755); err != nil {
		return "", fmt.Errorf("create download directory: %w", err)
	}

	f, err := os.CreateTemp("downloads", ".anvu-"+track.ID+"-*")
	if err != nil {
		return "", fmt.Errorf("create file: %w", err)
	}
	tmp := f.Name()
	defer os.Remove(tmp)

	// Peek at first 16 bytes to detect HTML / error-page responses before
	// writing the whole stream.
	var header [16]byte
	n, _ := io.ReadFull(reader, header[:])
	if n > 0 {
		sig := strings.ToLower(strings.TrimSpace(string(header[:n])))
		if strings.HasPrefix(sig, "<!doc") || strings.HasPrefix(sig, "<html") {
			_ = f.Close()
			return "", fmt.Errorf("received HTML error page instead of audio (magic: %x)", header[:n])
		}
		if _, err := f.Write(header[:n]); err != nil {
			_ = f.Close()
			return "", fmt.Errorf("write header: %w", err)
		}
	}

	if _, err := io.Copy(f, reader); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("write file: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("close file: %w", err)
	}

	if !fileExists(tmp) {
		return "", errors.New("empty file after download")
	}

	// Reject tiny files — likely error responses disguised as audio
	fi, err := os.Stat(tmp)
	if err != nil {
		return "", fmt.Errorf("stat file: %w", err)
	}
	if fi.Size() < 4096 {
		return "", fmt.Errorf("file too small (%d bytes) — likely not audio", fi.Size())
	}

	saveMu.Lock()
	defer saveMu.Unlock()

	if fileExists(path) {
		return path, nil
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("publish file: %w", err)
	}

	gologging.InfoF("ShrutiApi: downloaded %s -> %s", track.ID, path)
	return path, nil
}

// ── Key Pool (multi API key rotation) ───────────────────────

const (
	keyLimit  = 100            // ek key se kitni downloads
	keyWindow = 24 * time.Hour // itne time baad counter reset (API ke reset ke hisaab se badal lena)
)

var ErrAllKeysExhausted = errors.New("all API keys exhausted")

type keyState struct {
	key       string
	used      int
	exhausted bool
	resetAt   time.Time
}

type KeyPool struct {
	mu     sync.Mutex
	name   string
	keys   []*keyState
	limit  int
	window time.Duration
	next   int
}

func NewKeyPool(name string, keys []string, limit int, window time.Duration) *KeyPool {
	p := &KeyPool{name: name, limit: limit, window: window}
	for _, k := range keys {
		p.keys = append(p.keys, &keyState{key: k, resetAt: time.Now().Add(window)})
	}
	return p
}

// refresh resets counters whose window is over. Call with lock held.
func (p *KeyPool) refresh(now time.Time) {
	for _, k := range p.keys {
		if now.After(k.resetAt) {
			k.used = 0
			k.exhausted = false
			k.resetAt = now.Add(p.window)
		}
	}
}

// Available reports whether at least one key can still be used.
func (p *KeyPool) Available() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refresh(time.Now())
	for _, k := range p.keys {
		if !k.exhausted && k.used < p.limit {
			return true
		}
	}
	return false
}

// Acquire returns the next usable key (round-robin) and counts one use.
func (p *KeyPool) Acquire() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refresh(time.Now())

	n := len(p.keys)
	for i := 0; i < n; i++ {
		idx := (p.next + i) % n
		k := p.keys[idx]
		if k.exhausted || k.used >= p.limit {
			continue
		}
		k.used++
		p.next = (idx + 1) % n
		gologging.DebugF("KeyPool[%s]: key #%d used %d/%d", p.name, idx+1, k.used, p.limit)
		return k.key, nil
	}
	return "", ErrAllKeysExhausted
}

// Refund gives back one use (when the request got cancelled before finishing).
func (p *KeyPool) Refund(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, k := range p.keys {
		if k.key == key && k.used > 0 {
			k.used--
			return
		}
	}
}

// MarkExhausted disables a key until its window resets (server said quota/limit over).
func (p *KeyPool) MarkExhausted(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, k := range p.keys {
		if k.key == key {
			k.exhausted = true
			gologging.WarnF("KeyPool[%s]: key #%d exhausted, switching to next key", p.name, i+1)
			return
		}
	}
}

// Do runs fn with a key. If the server says the key's quota is over,
// it automatically retries with the next key.
func (p *KeyPool) Do(fn func(key string) (string, error)) (string, error) {
	var lastErr error
	for i := 0; i < len(p.keys); i++ {
		key, err := p.Acquire()
		if err != nil {
			if lastErr != nil {
				return "", lastErr
			}
			return "", err
		}
		path, err := fn(key)
		if err == nil {
			return path, nil
		}
		lastErr = err
		if errors.Is(err, context.Canceled) {
			p.Refund(key)
			return "", err
		}
		if isQuotaErr(err) {
			p.MarkExhausted(key)
			continue
		}
		return "", err
	}
	return "", lastErr
}

func isQuotaErr(err error) bool {
	if err == nil {
		return false
	}
	m := strings.ToLower(err.Error())
	for _, s := range []string{
		"http 429", "http 402", "http 403", "http 401",
		"quota", "rate limit", "limit exceeded", "limit reached", "limit over", "expired",
	} {
		if strings.Contains(m, s) {
			return true
		}
	}
	return false
}

// splitKeys: "k1,k2 k3\nk4" -> [k1 k2 k3 k4] (duplicates removed)
func splitKeys(raws ...string) []string {
	seen := map[string]bool{}
	var out []string
	for _, raw := range raws {
		f := strings.FieldsFunc(raw, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\n' || r == ';'
		})
		for _, k := range f {
			k = strings.TrimSpace(k)
			if k != "" && !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	return out
}

func getYukiKeys() []string {
	return splitKeys(config.YukiAPIKey, os.Getenv("YUKI_API_KEYS"), os.Getenv("YUKI_API_KEY"))
}

func getShrutiKeys() []string {
	return splitKeys(config.ShrutiAPIKey, os.Getenv("SHRUTI_API_KEYS"), os.Getenv("SHRUTI_API_KEY"))
}

var (
	yukiPoolOnce   sync.Once
	yukiKeyPool    *KeyPool
	shrutiPoolOnce sync.Once
	shrutiKeyPool  *KeyPool
)

func yukiPool() *KeyPool {
	yukiPoolOnce.Do(func() {
		yukiKeyPool = NewKeyPool("Yuki", getYukiKeys(), keyLimit, keyWindow)
	})
	return yukiKeyPool
}

func shrutiPool() *KeyPool {
	shrutiPoolOnce.Do(func() {
		shrutiKeyPool = NewKeyPool("Shruti", getShrutiKeys(), keyLimit, keyWindow)
	})
	return shrutiKeyPool
}
