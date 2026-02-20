/*
  - This file is part of YukkiMusic.
    *

  - YukkiMusic — A Telegram bot that streams music into group voice chats with seamless playback and control.
  - Copyright (C) 2025 TheTeamVivek
    *
  - This program is free software: you can redistribute it and/or modify
  - it under the terms of the GNU General Public License as published by
  - the Free Software Foundation, either version 3 of the License, or
  - (at your option) any later version.
    *
  - This program is distributed in the hope that it will be useful,
  - but WITHOUT ANY WARRANTY; without even the implied warranty of
  - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
  - GNU General Public License for more details.
    *
  - You should have received a copy of the GNU General Public License
  - along with this program. If not, see <https://www.gnu.org/licenses/>.
*/
package platforms

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/amarnathcjd/gogram/telegram"

	"main/internal/config"
	state "main/internal/core/models"
	"main/internal/utils"
)

type YouTubePlatform struct {
	name        state.PlatformName
	cookiesFile string
}

var (
	playlistRegex    = regexp.MustCompile(`(?i)(?:list=)([A-Za-z0-9_-]+)`)
	youtubeLinkRegex = regexp.MustCompile(
		`(?i)^(?:https?:\/\/)?(?:www\.|m\.|music\.)?(?:youtube\.com|youtu\.be)\/\S+`,
	)
	youtubeCache = utils.NewCache[string, []*state.Track](1 * time.Hour)
	cookiesPath  = "/app/cookies/cookies.txt" // Default path, can be changed via env
)

const (
	PlatformYouTube        state.PlatformName = "YouTube"
	innerTubeKey                              = "AIzaSyBOti4mM-6x9WDnZIjIeyEU21OpBXqWBgw"
	innerTubeClientVersion                    = "2.20250101.01.00"
	innerTubeClientName                       = "WEB"
)

var yt = &YouTubePlatform{
	name:        PlatformYouTube,
	cookiesFile: os.Getenv("YOUTUBE_COOKIES_FILE"),
}

func init() {
	if yt.cookiesFile == "" {
		yt.cookiesFile = cookiesPath
	}
	Register(90, yt)
}

func (yp *YouTubePlatform) Name() state.PlatformName {
	return yp.name
}

func (yp *YouTubePlatform) CanGetTracks(link string) bool {
	return youtubeLinkRegex.MatchString(link)
}

func (yp *YouTubePlatform) GetTracks(
	input string,
	video bool,
) ([]*state.Track, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, errors.New("empty query")
	}

	if youtubeLinkRegex.MatchString(trimmed) {

		if playlistRegex.MatchString(trimmed) {
			cacheKey := "playlist:" + strings.ToLower(trimmed)
			if cached, ok := youtubeCache.Get(cacheKey); ok {
				return updateCached(cached, video), nil
			}

			videoIDs, err := yp.getPlaylist(trimmed)
			if err != nil {
				return nil, err
			}

			var tracks []*state.Track
			for _, videoID := range videoIDs {
				if cached, ok := youtubeCache.Get("track:" + videoID); ok &&
					len(cached) > 0 {
					tracks = append(tracks, cached[0])
					continue
				}

				trackList, err := yp.VideoSearch(
					"https://youtube.com/watch?v="+videoID,
					true,
				)
				if err != nil || len(trackList) == 0 {
					continue
				}

				t := trackList[0]
				youtubeCache.Set("track:"+videoID, []*state.Track{t})
				tracks = append(tracks, t)
			}

			if len(tracks) > 0 {
				youtubeCache.Set(cacheKey, tracks)
			}

			return updateCached(tracks, video), nil
		}

		normalizedURL, videoID, err := yp.normalizeYouTubeURL(trimmed)
		if err != nil {
			return nil, err
		}

		if cached, ok := youtubeCache.Get("track:" + videoID); ok &&
			len(cached) > 0 {
			return updateCached(cached, video), nil
		}

		trackList, err := yp.VideoSearch(normalizedURL, true)
		if err != nil {
			return nil, err
		}
		if len(trackList) == 0 {
			return nil, errors.New("track not found for the given url")
		}

		youtubeCache.Set("track:"+videoID, trackList)
		return updateCached(trackList, video), nil
	}

	tracks, err := yp.VideoSearch(trimmed, true)
	if err != nil {
		return nil, err
	}
	if len(tracks) == 0 {
		return nil, errors.New("no tracks found for the given query")
	}

	return updateCached(tracks, video), nil
}

func (yp *YouTubePlatform) CanDownload(source state.PlatformName) bool {
	return false
}

func (yt *YouTubePlatform) Download(
	ctx context.Context,
	track *state.Track,
	mystic *telegram.NewMessage,
) (string, error) {
	return "", errors.New("youtube platform does not support downloading")
}

func (yp *YouTubePlatform) CanGetRecommendations() bool {
	return true
}

func (yp *YouTubePlatform) GetRecommendations(
	track *state.Track,
) ([]*state.Track, error) {
	nextURL := "https://m.youtube.com/youtubei/v1/next?key=" + innerTubeKey
	var result map[string]any

	// Create HTTP client with cookie support
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	reqBody := map[string]any{
		"context": map[string]any{
			"client": map[string]any{
				"clientName":       innerTubeClientName,
				"clientVersion":    innerTubeClientVersion,
				"hl":               "en",
				"gl":               "IN",
				"utcOffsetMinutes": 330,
			},
			"user": map[string]any{
				"lockedSafetyMode": false,
			},
		},
		"videoId":    track.ID,
		"playlistId": "RD" + track.ID,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", nextURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://www.youtube.com")
	req.Header.Set("Referer", "https://www.youtube.com/watch?v="+track.ID)

	// Add cookies if file exists
	if yp.cookiesFile != "" {
		if _, err := os.Stat(yp.cookiesFile); err == nil {
			// Read and parse cookies file
			cookies, err := yp.parseCookiesFile(yp.cookiesFile)
			if err == nil {
				for _, cookie := range cookies {
					req.AddCookie(cookie)
				}
			}
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	var tracks []*state.Track

	if playlist := dig(result, "contents", "twoColumnWatchNextResults", "playlist", "playlist", "contents"); playlist != nil {
		yp.parseNextResults(playlist, &tracks, track.Video, track.Requester)
	}

	if len(tracks) == 0 {
		return nil, errors.New("no recommendations found")
	}

	seen := make(map[string]bool)
	seen[track.ID] = true

	var filtered []*state.Track
	for _, t := range tracks {
		if t == nil {
			continue
		}
		if !seen[t.ID] {
			if t.Duration > 0 &&
				(config.DurationLimit < 0 || t.Duration <= config.DurationLimit) {
				filtered = append(filtered, t)
				seen[t.ID] = true
			}
		}
	}

	if len(filtered) == 0 {
		return nil, errors.New("no new recommendations found")
	}

	return filtered, nil
}

func (yp *YouTubePlatform) parseNextResults(
	node any,
	tracks *[]*state.Track,
	video bool,
	requester string,
) {
	switch v := node.(type) {
	case []any:
		for _, item := range v {
			yp.parseNextResults(item, tracks, video, requester)
		}
	case map[string]any:
		if _, ok := v["continuationItemRenderer"]; ok {
			return
		}
		if _, ok := v["itemSectionRenderer"]; ok {
			return
		}

		if vid, ok := dig(v, "playlistPanelVideoRenderer").(map[string]any); ok {
			id := safeString(vid["videoId"])
			title := safeString(dig(vid, "title", "simpleText"))
			if title == "" {
				title = safeString(dig(vid, "title", "runs", 0, "text"))
			}
			thumb := yp.getThumbnailURL(vid)
			durationText := safeString(dig(vid, "lengthText", "simpleText"))

			if durationText == "" || id == "" {
				return
			}

			duration := parseDuration(durationText)
			t := &state.Track{
				URL:       "https://www.youtube.com/watch?v=" + id,
				Title:     title,
				ID:        id,
				Artwork:   thumb,
				Duration:  duration,
				Source:    PlatformYouTube,
				Video:     video,
				Requester: requester,
			}
			*tracks = append(*tracks, t)
			youtubeCache.Set("track:"+t.ID, []*state.Track{t})
		} else if lockup, ok := dig(v, "lockupViewModel").(map[string]any); ok {
			contentType := safeString(lockup["contentType"])
			if contentType != "LOCKUP_CONTENT_TYPE_VIDEO" {
				return
			}

			id := safeString(lockup["contentId"])
			if id == "" {
				return
			}

			meta := dig(lockup, "metadata", "lockupMetadataViewModel")
			title := safeString(dig(meta, "title", "content"))

			thumbVM := dig(lockup, "contentImage", "thumbnailViewModel")
			sources, _ := dig(thumbVM, "image", "sources").([]any)
			thumb := ""
			if len(sources) > 0 {
				if lastSource, ok := sources[len(sources)-1].(map[string]any); ok {
					thumb = safeString(lastSource["url"])
				}
			}

			durationText := ""
			overlays, _ := dig(thumbVM, "overlays").([]any)
			for _, overlay := range overlays {
				if overlayMap, ok := overlay.(map[string]any); ok {
					if badgeVM, ok := dig(overlayMap, "thumbnailOverlayBadgeViewModel").(map[string]any); ok {
						badges, _ := dig(badgeVM, "thumbnailBadges").([]any)
						if len(badges) > 0 {
							if badge, ok := badges[0].(map[string]any); ok {
								if badgeData, ok := dig(badge, "thumbnailBadgeViewModel").(map[string]any); ok {
									durationText = safeString(badgeData["text"])
									break
								}
							}
						}
					}
				}
			}

			if durationText == "" || id == "" {
				return
			}

			duration := parseDuration(durationText)
			t := &state.Track{
				URL:       "https://www.youtube.com/watch?v=" + id,
				Title:     title,
				ID:        id,
				Artwork:   thumb,
				Duration:  duration,
				Source:    PlatformYouTube,
				Video:     video,
				Requester: requester,
			}
			*tracks = append(*tracks, t)
			youtubeCache.Set("track:"+t.ID, []*state.Track{t})
		} else if vid, ok := dig(v, "compactVideoRenderer").(map[string]any); ok {
			id := safeString(vid["videoId"])
			title := safeString(dig(vid, "title", "simpleText"))
			if title == "" {
				title = safeString(dig(vid, "title", "runs", 0, "text"))
			}
			thumb := yp.getThumbnailURL(vid)
			durationText := safeString(dig(vid, "lengthText", "simpleText"))

			if durationText == "" || id == "" {
				return
			}

			duration := parseDuration(durationText)
			t := &state.Track{
				URL:       "https://www.youtube.com/watch?v=" + id,
				Title:     title,
				ID:        id,
				Artwork:   thumb,
				Duration:  duration,
				Source:    PlatformYouTube,
				Video:     video,
				Requester: requester,
			}
			*tracks = append(*tracks, t)
			youtubeCache.Set("track:"+t.ID, []*state.Track{t})
		} else {
			for _, child := range v {
				yp.parseNextResults(child, tracks, video, requester)
			}
		}
	}
}

func (*YouTubePlatform) CanSearch() bool { return true }

func (y *YouTubePlatform) Search(
	q string,
	video bool,
) ([]*state.Track, error) {
	return y.GetTracks(q, video)
}

func (yp *YouTubePlatform) VideoSearch(
	query string,
	singleOpt ...bool,
) ([]*state.Track, error) {
	single := false
	if len(singleOpt) > 0 && singleOpt[0] {
		single = true
	}

	cacheKey := "search:" + strings.TrimSpace(strings.ToLower(query))
	if arr, ok := youtubeCache.Get(cacheKey); ok {
		if single && len(arr) > 0 {
			return []*state.Track{arr[0]}, nil
		}
		if !single && len(arr) == 1 {
			// goto Search
		} else {
			return arr, nil
		}
	}

	var tracks []*state.Track
	var err error

	tracks, err = yp.searchYouTube(query)
	if err != nil {
		return nil, fmt.Errorf("ytsearch failed: %w", err)
	}

	if len(tracks) == 0 {
		return nil, errors.New("no tracks found")
	}

	youtubeCache.Set(cacheKey, tracks)

	if single {
		return []*state.Track{tracks[0]}, nil
	}

	return tracks, nil
}

func (yp *YouTubePlatform) normalizeYouTubeURL(
	input string,
) (string, string, error) {
	u, err := url.Parse(strings.TrimSpace(input))
	if err != nil {
		return "", "", err
	}

	host := strings.ToLower(u.Host)
	path := strings.Trim(u.Path, "/")

	if strings.Contains(host, "youtu.be") {
		id := strings.Split(path, "/")[0]
		if len(id) == 11 {
			return "https://www.youtube.com/watch?v=" + id, id, nil
		}
	}

	if strings.Contains(host, "youtube.com") {
		if v := u.Query().Get("v"); len(v) == 11 {
			return "https://www.youtube.com/watch?v=" + v, v, nil
		}

		parts := strings.Split(path, "/")

		if len(parts) >= 2 && parts[0] == "shorts" && len(parts[1]) == 11 {
			return "https://www.youtube.com/watch?v=" + parts[1], parts[1], nil
		}

		if len(parts) >= 3 && parts[0] == "source" && len(parts[1]) == 11 {
			return "https://www.youtube.com/watch?v=" + parts[1], parts[1], nil
		}

		if len(parts) >= 2 && parts[0] == "embed" && len(parts[1]) == 11 {
			return "https://www.youtube.com/watch?v=" + parts[1], parts[1], nil
		}
	}

	return "", "", errors.New("unsupported YouTube URL or missing video ID")
}

func (yp *YouTubePlatform) getPlaylist(pUrl string) ([]string, error) {
	if strings.Contains(pUrl, "&") {
		pUrl = strings.Split(pUrl, "&")[0]
	}

	args := []string{
		"-i",
		"--compat-options",
		"no-youtube-unavailable-videos",
		"--get-id",
		"--flat-playlist",
		"--skip-download",
		"--playlist-end",
		strconv.Itoa(config.QueueLimit),
	}

	// Add cookies if file exists
	if yp.cookiesFile != "" {
		if _, err := os.Stat(yp.cookiesFile); err == nil {
			args = append(args, "--cookies", yp.cookiesFile)
		}
	}

	args = append(args, pUrl)

	cmd := exec.Command("yt-dlp", args...)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("yt-dlp error: %v\n%s", err, stderr.String())
	}

	return strings.Split(strings.TrimSpace(out.String()), "\n"), nil
}

func (yp *YouTubePlatform) parseCookiesFile(path string) ([]*http.Cookie, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cookies []*http.Cookie
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Split(line, "\t")
		if len(parts) < 7 {
			continue
		}

		// Netscape cookie format
		cookie := &http.Cookie{
			Domain:   parts[0],
			Path:     parts[2],
			Secure:   parts[3] == "TRUE",
			Expires:  time.Unix(parseInt64(parts[4]), 0),
			Name:     parts[5],
			Value:    parts[6],
			HttpOnly: false,
		}

		cookies = append(cookies, cookie)
	}

	return cookies, nil
}

func updateCached(arr []*state.Track, video bool) []*state.Track {
	if len(arr) == 0 {
		return nil
	}
	out := make([]*state.Track, len(arr))
	for i, t := range arr {
		if t == nil {
			continue
		}
		clone := *t
		clone.Video = video
		out[i] = &clone
	}
	return out
}

func (yp *YouTubePlatform) searchYouTube(query string) ([]*state.Track, error) {
	searchURL := "https://m.youtube.com/youtubei/v1/search?key=" + innerTubeKey
	var result map[string]any

	// Create HTTP client with cookie support
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	reqBody := map[string]any{
		"context": map[string]any{
			"client": map[string]any{
				"clientName":       innerTubeClientName,
				"clientVersion":    innerTubeClientVersion,
				"newVisitorCookie": true,
				"acceptHeader":     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
				"hl":               "en-IN",
				"gl":               "IN",
			},
		},
		"request": map[string]any{
			"useSsl": true,
		},
		"user": map[string]any{
			"lockedSafetyMode": false,
		},
		"params": "CAASAhAB",
		"query":  query,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", searchURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-origin", "https://m.youtube.com")
	req.Header.Set("origin", "https://m.youtube.com")
	req.Header.Set("accept-language", "en-IN")

	// Add cookies if file exists
	if yp.cookiesFile != "" {
		if _, err := os.Stat(yp.cookiesFile); err == nil {
			cookies, err := yp.parseCookiesFile(yp.cookiesFile)
			if err == nil {
				for _, cookie := range cookies {
					req.AddCookie(cookie)
				}
			}
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	contents := dig(
		result,
		"contents",
		"twoColumnSearchResultsRenderer",
		"primaryContents",
		"sectionListRenderer",
		"contents",
	)
	if contents == nil {
		return nil, fmt.Errorf("no contents found")
	}

	var tracks []*state.Track
	yp.parseSearchResults(contents, &tracks)
	return tracks, nil
}

func (yp *YouTubePlatform) parseSearchResults(node any, tracks *[]*state.Track) {
	switch v := node.(type) {

	case []any:
		for _, item := range v {
			yp.parseSearchResults(item, tracks)
		}

	case map[string]any:
		if vid, ok := dig(v, "videoRenderer").(map[string]any); ok {

			if yp.isLiveVideo(vid) {
				return
			}

			id := safeString(vid["videoId"])
			title := safeString(dig(vid, "title", "runs", 0, "text"))
			thumb := yp.getThumbnailURL(vid)
			durationText := safeString(dig(vid, "lengthText", "simpleText"))

			if durationText == "" {
				return
			}

			duration := parseDuration(durationText)
			t := &state.Track{
				URL:      "https://www.youtube.com/watch?v=" + id,
				Title:    title,
				ID:       id,
				Artwork:  thumb,
				Duration: duration,
				Source:   PlatformYouTube,
			}
			*tracks = append(*tracks, t)
			youtubeCache.Set("track:"+t.ID, []*state.Track{t})
		} else {
			for _, child := range v {
				yp.parseSearchResults(child, tracks)
			}
		}
	}
}

func (yp *YouTubePlatform) isLiveVideo(videoRenderer map[string]any) bool {
	if badges, ok := dig(videoRenderer, "badges").([]any); ok {
		for _, badge := range badges {
			if badgeMap, ok := badge.(map[string]any); ok {
				if metadataBadge, ok := dig(badgeMap, "metadataBadgeRenderer").(map[string]any); ok {

					style := safeString(metadataBadge["style"])
					label := safeString(metadataBadge["label"])

					if style == "BADGE_STYLE_TYPE_LIVE_NOW" || label == "LIVE" {
						return true
					}
				}
			}
		}
	}

	if viewCountText, ok := dig(videoRenderer, "viewCountText", "runs").([]any); ok {
		for _, run := range viewCountText {
			if runMap, ok := run.(map[string]any); ok {
				text := safeString(runMap["text"])
				if strings.Contains(strings.ToLower(text), "watching") {
					return true
				}
			}
		}
	}
	return false
}

func (yp *YouTubePlatform) getThumbnailURL(vid map[string]any) string {
	thumbs, ok := dig(vid, "thumbnail", "thumbnails").([]any)
	if !ok || len(thumbs) == 0 {
		return ""
	}

	last := thumbs[len(thumbs)-1]
	if m, ok := last.(map[string]any); ok {
		return safeString(m["url"])
	}
	return ""
}

func dig(m any, path ...any) any {
	curr := m
	for _, p := range path {
		switch key := p.(type) {
		case string:
			if mm, ok := curr.(map[string]any); ok {
				curr = mm[key]
			} else {
				return nil
			}
		case int:
			if arr, ok := curr.([]any); ok && len(arr) > key {
				curr = arr[key]
			} else {
				return nil
			}
		}
	}
	return curr
}

func safeString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func parseDuration(s string) int {
	if s == "" {
		return 0
	}
	parts := strings.Split(s, ":")
	total := 0
	multiplier := 1
	for i := len(parts) - 1; i >= 0; i-- {
		total += atoi(parts[i]) * multiplier
		multiplier *= 60
	}
	return total
}

func atoi(s string) int {
	var n int
	for _, r := range s {
		if r >= '0' && r <= '9' {
			n = n*10 + int(r-'0')
		}
	}
	return n
}

func parseInt64(s string) int64 {
	var n int64
	for _, r := range s {
		if r >= '0' && r <= '9' {
			n = n*10 + int64(r-'0')
		}
	}
	return n
}
