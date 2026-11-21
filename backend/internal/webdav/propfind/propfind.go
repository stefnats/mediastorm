package propfind

// Code copied from webdav package of golang.org/x/net/webdav to override

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"golang.org/x/net/webdav"
)

const (
	infiniteDepth = -1
	invalidDepth  = -2
)

var (
	errInvalidDepth    = errors.New("webdav: invalid depth")
	errInvalidPropfind = errors.New("webdav: invalid propfind")
	errInvalidResponse = errors.New("webdav: invalid response")
	errPrefixMismatch  = errors.New("webdav: prefix mismatch")
)

func HandlePropfind(fs webdav.FileSystem, ls webdav.LockSystem, w http.ResponseWriter, r *http.Request, prefix string) (status int, err error) {
	reqPath, status, err := stripPrefix(r.URL.Path, prefix)
	if err != nil {
		return status, err
	}

	ctx := r.Context()
	fi, err := fs.Stat(ctx, reqPath)
	if err != nil {
		if os.IsNotExist(err) {
			return http.StatusNotFound, err
		}
		return http.StatusMethodNotAllowed, err
	}
	depth := infiniteDepth
	if hdr := r.Header.Get("Depth"); hdr != "" {
		depth = parseDepth(hdr)
		if depth == invalidDepth {
			return http.StatusBadRequest, errInvalidDepth
		}
	}
	pf, status, err := readPropfind(r.Body)
	if err != nil {
		return status, err
	}

	mw := multistatusWriter{w: w}

	walkFn := func(reqPath string, info os.FileInfo, err error) error {
		if err != nil {
			return handlePropfindError(err, info)
		}

		var pstats []webdav.Propstat
		if pf.Propname != nil {
			pnames, err := propnames(info)
			if err != nil {
				return handlePropfindError(err, info)
			}
			pstat := webdav.Propstat{Status: http.StatusOK}
			for _, xmlname := range pnames {
				pstat.Props = append(pstat.Props, webdav.Property{XMLName: xmlname})
			}
			pstats = append(pstats, pstat)
		} else if pf.Allprop != nil {
			pstats, err = allprop(ctx, info, reqPath, pf.Prop)
		} else {
			pstats, err = props(ctx, info, reqPath, pf.Prop)
		}
		if err != nil {
			return handlePropfindError(err, info)
		}
		href := path.Join(prefix, reqPath)
		if href != "/" && info.IsDir() {
			href += "/"
		}

		return mw.write(makePropstatResponse(href, pstats))
	}

	walkErr := walkFS(ctx, fs, depth, reqPath, fi, walkFn)
	closeErr := mw.close()
	if walkErr != nil {
		return http.StatusInternalServerError, walkErr
	}
	if closeErr != nil {
		return http.StatusInternalServerError, closeErr
	}
	return 0, nil
}

// parseDepth maps the strings "0", "1" and "infinity" to 0, 1 and
// infiniteDepth. Parsing any other string returns invalidDepth.
//
// Different WebDAV methods have further constraints on valid depths:
//   - PROPFIND has no further restrictions, as per section 9.1.
//   - COPY accepts only "0" or "infinity", as per section 9.8.3.
//   - MOVE accepts only "infinity", as per section 9.9.2.
//   - LOCK accepts only "0" or "infinity", as per section 9.10.3.
//
// These constraints are enforced by the handleXxx methods.
func parseDepth(s string) int {
	switch s {
	case "0":
		return 0
	case "1":
		return 1
	case "infinity":
		return infiniteDepth
	}
	return invalidDepth
}

func handlePropfindError(err error, info os.FileInfo) error {
	var skipResp error = nil
	if info != nil && info.IsDir() {
		skipResp = filepath.SkipDir
	}

	if errors.Is(err, os.ErrPermission) {
		// If the server cannot recurse into a directory because it is not allowed,
		// then there is nothing more to say about it. Just skip sending anything.
		return skipResp
	}

	if _, ok := err.(*os.PathError); ok {
		// If the file is just bad, it couldn't be a proper WebDAV resource. Skip it.
		return skipResp
	}

	// We need to be careful with other errors: there is no way to abort the xml stream
	// part way through while returning a valid PROPFIND response. Returning only half
	// the data would be misleading, but so would be returning results tainted by errors.
	// The current behaviour by returning an error here leads to the stream being aborted,
	// and the parent http server complaining about writing a spurious header. We should
	// consider further enhancing this error handling to more gracefully fail, or perhaps
	// buffer the entire response until we've walked the tree.
	return err
}

func makePropstatResponse(href string, pstats []webdav.Propstat) *response {
	resp := response{
		Href:     []string{(&url.URL{Path: href}).EscapedPath()},
		Propstat: make([]propstat, 0, len(pstats)),
	}
	for _, p := range pstats {
		var xmlErr *xmlError
		if p.XMLError != "" {
			xmlErr = &xmlError{InnerXML: []byte(p.XMLError)}
		}
		resp.Propstat = append(resp.Propstat, propstat{
			Status:              fmt.Sprintf("HTTP/1.1 %d %s", p.Status, webdav.StatusText(p.Status)),
			Prop:                p.Props,
			ResponseDescription: p.ResponseDescription,
			Error:               xmlErr,
		})
	}
	return &resp
}

// walkFS traverses filesystem fs starting at name up to depth levels.
//
// Allowed values for depth are 0, 1 or infiniteDepth. For each visited node,
// walkFS calls walkFn. If a visited file system node is a directory and
// walkFn returns filepath.SkipDir, walkFS will skip traversal of this node.
func walkFS(ctx context.Context, fs webdav.FileSystem, depth int, name string, info os.FileInfo, walkFn filepath.WalkFunc) error {
	// This implementation is based on Walk's code in the standard path/filepath package.
	err := walkFn(name, info, nil)
	if err != nil {
		if info.IsDir() && err == filepath.SkipDir {
			return nil
		}
		return err
	}
	if !info.IsDir() || depth == 0 {
		return nil
	}
	if depth == 1 {
		depth = 0
	}

	// Read directory names.
	f, err := fs.OpenFile(ctx, name, os.O_RDONLY, 0)
	if err != nil {
		return walkFn(name, info, err)
	}
	fileInfos, err := f.Readdir(0)
	f.Close()
	if err != nil {
		return walkFn(name, info, err)
	}

	for _, fileInfo := range fileInfos {
		filename := path.Join(name, fileInfo.Name())

		err = walkFS(ctx, fs, depth, filename, fileInfo, walkFn)
		if err != nil {
			if !fileInfo.IsDir() || err != filepath.SkipDir {
				return err
			}
		}
	}
	return nil
}

func stripPrefix(p, prefix string) (string, int, error) {
	if prefix == "" {
		return p, http.StatusOK, nil
	}
	if r := strings.TrimPrefix(p, prefix); len(r) < len(p) {
		return r, http.StatusOK, nil
	}
	return p, http.StatusNotFound, errPrefixMismatch
}

// HandleBoundedPropfind stages one complete managed response. Its filesystem
// must be the original bounded request source, never a generic fallback.
func HandleBoundedPropfind(fs webdav.FileSystem, ls webdav.LockSystem, w http.ResponseWriter, r *http.Request, prefix, hrefPrefix string) (int, error) {
	if hrefPrefix == "" || len(hrefPrefix) > 8192 || !strings.HasPrefix(hrefPrefix, "/") || strings.HasPrefix(hrefPrefix, "//") || path.Clean(hrefPrefix) != hrefPrefix || strings.ContainsAny(hrefPrefix, "\\\x00\r\n?#") {
		return http.StatusBadRequest, errInvalidResponse
	}
	reqPath, status, err := stripPrefix(r.URL.Path, prefix)
	if err != nil {
		return status, err
	}
	depth := infiniteDepth
	if hdr := r.Header.Get("Depth"); hdr != "" {
		depth = parseDepth(hdr)
		if depth == invalidDepth {
			return http.StatusBadRequest, errInvalidDepth
		}
	}
	// Read the original bounded body before any source enumeration.
	pf, status, err := readPropfind(r.Body)
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			return http.StatusRequestEntityTooLarge, err
		}
		return status, err
	}
	if err = r.Context().Err(); err != nil {
		return http.StatusServiceUnavailable, err
	}
	info, err := fs.Stat(r.Context(), reqPath)
	if err != nil {
		if os.IsNotExist(err) {
			return http.StatusNotFound, err
		}
		return http.StatusServiceUnavailable, err
	}
	out := &boundedPropfindOutput{header: make(http.Header)}
	mw := multistatusWriter{w: out}
	visited := 0
	var walk func(string, os.FileInfo, int, int) error
	walk = func(name string, fi os.FileInfo, left, level int) error {
		if err := r.Context().Err(); err != nil {
			return err
		}
		if level > 32 || visited >= 4096 {
			return errManagedPropfindBound
		}
		visited++
		var stats []webdav.Propstat
		if pf.Propname != nil {
			names, e := propnames(fi)
			if e != nil {
				return e
			}
			st := webdav.Propstat{Status: http.StatusOK}
			for _, n := range names {
				st.Props = append(st.Props, webdav.Property{XMLName: n})
			}
			stats = append(stats, st)
		} else if pf.Allprop != nil {
			stats, err = allprop(r.Context(), fi, name, pf.Prop)
		} else {
			stats, err = props(r.Context(), fi, name, pf.Prop)
		}
		if err != nil {
			return err
		}
		href := path.Join(hrefPrefix, name)
		if href != "/" && fi.IsDir() {
			href += "/"
		}
		if err := mw.write(makePropstatResponse(href, stats)); err != nil {
			return err
		}
		if !fi.IsDir() || left == 0 {
			return nil
		}
		file, e := fs.OpenFile(r.Context(), name, os.O_RDONLY, 0)
		if e != nil {
			return e
		}
		children, e := file.Readdir(0)
		closeErr := file.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
		if len(children) > 4096-visited {
			return errManagedPropfindBound
		}
		next := left
		if next == 1 {
			next = 0
		}
		for _, child := range children {
			if e = walk(path.Join(name, child.Name()), child, next, level+1); e != nil {
				return e
			}
		}
		return nil
	}
	if err = walk(reqPath, info, depth, 0); err != nil {
		return http.StatusServiceUnavailable, err
	}
	if err = mw.close(); err != nil {
		return http.StatusServiceUnavailable, err
	}
	if err = r.Context().Err(); err != nil {
		return http.StatusServiceUnavailable, err
	}
	for key, values := range out.header {
		w.Header()[key] = append([]string(nil), values...)
	}
	w.Header().Set("Content-Length", fmt.Sprint(out.body.Len()))
	w.WriteHeader(webdav.StatusMulti)
	if _, err = w.Write(out.body.Bytes()); err != nil {
		return 0, err
	}
	return 0, nil
}

var errManagedPropfindBound = errors.New("bounded WebDAV metadata unavailable")

type boundedPropfindOutput struct {
	header http.Header
	body   bytes.Buffer
}

func (w *boundedPropfindOutput) Header() http.Header { return w.header }
func (w *boundedPropfindOutput) WriteHeader(int)     {}
func (w *boundedPropfindOutput) Write(p []byte) (int, error) {
	if len(p) > 8<<20-w.body.Len() {
		return 0, errManagedPropfindBound
	}
	return w.body.Write(p)
}

var _ io.Writer = (*boundedPropfindOutput)(nil)
