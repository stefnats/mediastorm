package localmedia

import (
	"bytes"
	"encoding/binary"
	"io"
)

// This parser consumes an already-authorized ReaderAt. A detected format is
// never an input opener or an imported source permission.
type familyLocalProbeFormatReader struct {
	reader        io.ReaderAt
	size, used    int64
	boxes, tracks int
}

func (r *familyLocalProbeFormatReader) read(at int64, n int) ([]byte, error) {
	if n <= 0 || n > 32<<10 || at < 0 || at > r.size || int64(n) > r.size-at || r.used+int64(n) > 1<<20 {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	r.used += int64(n)
	b := make([]byte, n)
	got, err := r.reader.ReadAt(b, at)
	if got != n || err != nil && err != io.EOF {
		clear(b)
		return nil, ErrFamilyLocalProbeUnavailable
	}
	return b, nil
}
func detectFamilyLocalProbeFormat(reader io.ReaderAt, size int64) (string, error) {
	if reader == nil || size <= 0 || size > familyLocalProbeMaxInput {
		return "", ErrFamilyLocalProbeUnavailable
	}
	r := &familyLocalProbeFormatReader{reader: reader, size: size}
	head, err := r.read(0, int(min(size, 4096)))
	if err != nil {
		return "", err
	}
	defer clear(head)
	if len(head) >= 564 && head[0] == 0x47 && head[188] == 0x47 && head[376] == 0x47 {
		return "mpegts", nil
	}
	if len(head) >= 4 && bytes.Equal(head[:4], []byte{0x1a, 0x45, 0xdf, 0xa3}) && familyLocalProbeEBML(head) {
		return "matroska", nil
	}
	if len(head) < 16 || string(head[4:8]) != "ftyp" {
		return "", ErrFamilyLocalProbeUnavailable
	}
	moov, mdat := 0, 0
	_, err = r.walk(0, size, 0, false, &moov, &mdat)
	if err != nil || moov != 1 || mdat == 0 || r.tracks == 0 {
		return "", ErrFamilyLocalProbeUnavailable
	}
	return "mov", nil
}
func familyLocalProbeEBML(b []byte) bool {
	vint := func(at int, id bool) (uint64, int, bool) {
		if at >= len(b) || b[at] == 0 {
			return 0, 0, false
		}
		mask := byte(0x80)
		n := 1
		for b[at]&mask == 0 {
			mask >>= 1
			n++
			if n > 8 {
				return 0, 0, false
			}
		}
		if id && n > 4 || at+n > len(b) {
			return 0, 0, false
		}
		v := uint64(b[at])
		if !id {
			v &= uint64(mask - 1)
		}
		for i := 1; i < n; i++ {
			v = v<<8 | uint64(b[at+i])
		}
		return v, n, true
	}
	n, skip, ok := vint(4, false)
	if !ok || n > 4096 || uint64(4+skip)+n > uint64(len(b)) {
		return false
	}
	end := 4 + skip + int(n)
	for at := 4 + skip; at < end; {
		id, s, ok := vint(at, true)
		if !ok || s > end-at {
			return false
		}
		at += s
		size, s, ok := vint(at, false)
		if !ok || s > end-at || size > uint64(end-at-s) {
			return false
		}
		at += s
		if id == 0x4282 {
			return size <= 16 && (string(b[at:at+int(size)]) == "matroska" || string(b[at:at+int(size)]) == "webm")
		}
		at += int(size)
	}
	return false
}
func (r *familyLocalProbeFormatReader) walk(begin, end int64, depth int, inTrack bool, moov, mdat *int) (int, error) {
	if depth > 16 {
		return 0, ErrFamilyLocalProbeUnavailable
	}
	drefs := 0
	for at := begin; at < end; {
		r.boxes++
		if r.boxes > 4096 || end-at < 8 {
			return 0, ErrFamilyLocalProbeUnavailable
		}
		header, err := r.read(at, 8)
		if err != nil {
			return 0, err
		}
		kind := string(header[4:8])
		size := uint64(binary.BigEndian.Uint32(header))
		clear(header)
		width := int64(8)
		if size == 1 {
			large, e := r.read(at+8, 8)
			if e != nil {
				return 0, e
			}
			size = binary.BigEndian.Uint64(large)
			clear(large)
			width = 16
		}
		if size == 0 {
			if kind != "mdat" {
				return 0, ErrFamilyLocalProbeUnavailable
			}
			size = uint64(end - at)
		}
		if size < uint64(width) || size > uint64(end-at) {
			return 0, ErrFamilyLocalProbeUnavailable
		}
		next := at + int64(size)
		switch kind {
		case "ftyp":
			if depth != 0 || at != 0 || size < 16 || size > 4096 || size%4 != 0 {
				return 0, ErrFamilyLocalProbeUnavailable
			}
		case "mdat":
			if depth != 0 {
				return 0, ErrFamilyLocalProbeUnavailable
			}
			*mdat++
		case "moov":
			if depth != 0 {
				return 0, ErrFamilyLocalProbeUnavailable
			}
			*moov++
			if *moov > 1 {
				return 0, ErrFamilyLocalProbeUnavailable
			}
			_, err = r.walk(at+width, next, depth+1, false, moov, mdat)
		case "trak":
			if inTrack || depth != 1 {
				return 0, ErrFamilyLocalProbeUnavailable
			}
			r.tracks++
			if r.tracks > 128 {
				return 0, ErrFamilyLocalProbeUnavailable
			}
			var count int
			count, err = r.walk(at+width, next, depth+1, true, moov, mdat)
			if err == nil && count != 1 {
				err = ErrFamilyLocalProbeUnavailable
			}
		case "mdia", "minf", "dinf":
			var count int
			count, err = r.walk(at+width, next, depth+1, inTrack, moov, mdat)
			drefs += count
		case "dref":
			if !inTrack || size < 16 {
				return 0, ErrFamilyLocalProbeUnavailable
			}
			body, e := r.read(at+width, 8)
			if e != nil {
				return 0, e
			}
			count := binary.BigEndian.Uint32(body[4:])
			validVersion := binary.BigEndian.Uint32(body[:4]) == 0
			clear(body)
			if !validVersion || count == 0 || count > 64 || int64(size)-width-8 != int64(count)*12 {
				return 0, ErrFamilyLocalProbeUnavailable
			}
			for i := uint32(0); i < count; i++ {
				entry, e := r.read(at+width+8+int64(i)*12, 12)
				if e != nil {
					return 0, e
				}
				valid := binary.BigEndian.Uint32(entry) == 12 && string(entry[4:8]) == "url " && binary.BigEndian.Uint32(entry[8:]) == 1
				clear(entry)
				if !valid {
					return 0, ErrFamilyLocalProbeUnavailable
				}
			}
			drefs++
		case "moof", "cmov", "rmra", "rdrf", "url ", "urn ":
			return 0, ErrFamilyLocalProbeUnavailable
		}
		if err != nil {
			return 0, err
		}
		at = next
	}
	return drefs, nil
}
