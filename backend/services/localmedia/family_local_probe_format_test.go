package localmedia

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func familyProbeBox(kind string, body ...[]byte) []byte {
	size := 8
	for _, b := range body {
		size += len(b)
	}
	out := make([]byte, size)
	binary.BigEndian.PutUint32(out, uint32(size))
	copy(out[4:8], kind)
	at := 8
	for _, b := range body {
		copy(out[at:], b)
		at += len(b)
	}
	return out
}
func familyProbeMovieBoxes(selfContained bool) ([]byte, []byte) {
	entry := make([]byte, 12)
	binary.BigEndian.PutUint32(entry, 12)
	copy(entry[4:8], "url ")
	if selfContained {
		binary.BigEndian.PutUint32(entry[8:], 1)
	}
	full := make([]byte, 8)
	binary.BigEndian.PutUint32(full[4:], 1)
	dref := familyProbeBox("dref", full, entry)
	track := familyProbeBox("trak", familyProbeBox("mdia", familyProbeBox("minf", familyProbeBox("dinf", dref))))
	return familyProbeBox("ftyp", []byte("isom\x00\x00\x02\x00")), familyProbeBox("moov", track)
}

type familyProbeCountReader struct {
	reader         io.ReaderAt
	bytes          int64
	calls, maxRead int
}

func (r *familyProbeCountReader) ReadAt(p []byte, at int64) (int, error) {
	r.bytes += int64(len(p))
	r.calls++
	r.maxRead = max(r.maxRead, len(p))
	return r.reader.ReadAt(p, at)
}

func TestFamilyLocalProbeFormatSafeTSMatroskaAndSelfContainedMovie(t *testing.T) {
	ts := make([]byte, 564)
	ts[0] = 0x47
	ts[188] = 0x47
	ts[376] = 0x47
	ebml := []byte{0x1a, 0x45, 0xdf, 0xa3, 0x8b, 0x42, 0x82, 0x88, 'm', 'a', 't', 'r', 'o', 's', 'k', 'a'}
	ftyp, moov := familyProbeMovieBoxes(true)
	for _, test := range []struct {
		want string
		data []byte
	}{{"mpegts", ts}, {"matroska", ebml}, {"mov", bytes.Join([][]byte{ftyp, familyProbeBox("mdat", []byte{0, 1, 2}), moov}, nil)}} {
		r := &familyProbeCountReader{reader: bytes.NewReader(test.data)}
		got, err := detectFamilyLocalProbeFormat(r, int64(len(test.data)))
		if err != nil || got != test.want || r.maxRead > 32<<10 || r.bytes > 1<<20 {
			t.Fatal("safe format rejected or budget exceeded", test.want, err)
		}
	}
}

func TestFamilyLocalProbeSparseMultiGiBTailMovieUsesBoundedReadAt(t *testing.T) {
	ftyp, moov := familyProbeMovieBoxes(true)
	file, err := os.Create(filepath.Join(t.TempDir(), "synthetic-sparse-movie.mp4"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	mdatSize := int64(5<<30) + 16
	tail := int64(len(ftyp)) + mdatSize
	if err = file.Truncate(tail + int64(len(moov))); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 16)
	binary.BigEndian.PutUint32(header, 1)
	copy(header[4:8], "mdat")
	binary.BigEndian.PutUint64(header[8:], uint64(mdatSize))
	for _, part := range []struct {
		at   int64
		data []byte
	}{{0, ftyp}, {int64(len(ftyp)), header}, {tail, moov}} {
		if _, err = file.WriteAt(part.data, part.at); err != nil {
			t.Fatal(err)
		}
	}
	r := &familyProbeCountReader{reader: file}
	got, err := detectFamilyLocalProbeFormat(r, tail+int64(len(moov)))
	if err != nil || got != "mov" || r.bytes > 8192 || r.maxRead > 4096 || r.calls > 32 {
		t.Fatal("sparse tail parser read payload or lost 64-bit position", r.bytes, r.calls, err)
	}
	// The cap is positive and genuinely multiGiB, but a larger input is denied
	// before asking even the first header reader.
	r = &familyProbeCountReader{reader: file}
	if _, err = detectFamilyLocalProbeFormat(r, familyLocalProbeMaxInput+1); err == nil || r.calls != 0 {
		t.Fatal("8 GiB input cap did not precede IO")
	}
}

func TestFamilyLocalProbeFormatRejectsReferencesOverflowAndMissingProof(t *testing.T) {
	ftyp, moov := familyProbeMovieBoxes(true)
	_, external := familyProbeMovieBoxes(false)
	movie := func(parts ...[]byte) []byte { return bytes.Join(append([][]byte{ftyp}, parts...), nil) }
	large := make([]byte, 16)
	binary.BigEndian.PutUint32(large, 1)
	copy(large[4:8], "mdat")
	binary.BigEndian.PutUint64(large[8:], ^uint64(0))
	deep := familyProbeBox("dref", make([]byte, 8))
	for range 18 {
		deep = familyProbeBox("dinf", deep)
	}
	duplicate := append([]byte{}, moov...)
	duplicate = append(duplicate, moov...)
	cases := map[string][]byte{
		"manifest": []byte("#EXTM3U\nhttps://invalid/secret\n"), "concat": []byte("ffconcat version 1.0\nfile secret\n"),
		"missing-moov": movie(familyProbeBox("mdat", []byte{1})), "missing-mdat": movie(moov), "external-dref": movie(external, familyProbeBox("mdat", []byte{1})),
		"missing-track": movie(familyProbeBox("moov"), familyProbeBox("mdat")), "missing-dref": movie(familyProbeBox("moov", familyProbeBox("trak", familyProbeBox("mdia"))), familyProbeBox("mdat")),
		"duplicate-moov": movie(duplicate, familyProbeBox("mdat")), "fragmented": movie(moov, familyProbeBox("moof"), familyProbeBox("mdat")),
		"extended-overflow": movie(large), "truncated": movie(moov, []byte{0, 0, 0, 16, 'm', 'd', 'a', 't'}), "depth": movie(familyProbeBox("moov", familyProbeBox("trak", deep)), familyProbeBox("mdat")),
		"box-count":     movie(bytes.Repeat(familyProbeBox("free"), 4097), moov, familyProbeBox("mdat")),
		"bad-ebml-size": {0x1a, 0x45, 0xdf, 0xa3, 0xff, 0x42, 0x82, 0x88}, "bad-ebml-vint": {0x1a, 0x45, 0xdf, 0xa3, 0x00},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := detectFamilyLocalProbeFormat(bytes.NewReader(data), int64(len(data))); err == nil {
				t.Fatal("unsafe format accepted")
			}
		})
	}
	for _, size := range []int64{-1, 0, familyLocalProbeMaxInput + 1} {
		if _, err := detectFamilyLocalProbeFormat(bytes.NewReader(ftyp), size); err == nil {
			t.Fatal("invalid size accepted")
		}
	}
	if _, err := detectFamilyLocalProbeFormat(nil, 16); err == nil {
		t.Fatal("missing source accepted")
	}
}

func TestFamilyLocalProbeFormatReadBudgetAndTruncationAreFailClosed(t *testing.T) {
	r := &familyLocalProbeFormatReader{reader: bytes.NewReader(make([]byte, 32<<10)), size: familyLocalProbeMaxInput}
	for range 32 {
		b, err := r.read(0, 32<<10)
		if err != nil {
			t.Fatal(err)
		}
		clear(b)
	}
	if _, err := r.read(0, 1); err == nil {
		t.Fatal("1 MiB preflight budget exceeded")
	}
	for _, span := range []struct {
		at int64
		n  int
	}{{-1, 1}, {familyLocalProbeMaxInput, 1}, {0, 32<<10 + 1}, {0, 0}, {familyLocalProbeMaxInput - 1, 2}} {
		r := &familyLocalProbeFormatReader{reader: bytes.NewReader(make([]byte, 16)), size: familyLocalProbeMaxInput}
		if _, err := r.read(span.at, span.n); err == nil {
			t.Fatal("unsafe read accepted")
		}
	}
	r = &familyLocalProbeFormatReader{reader: bytes.NewReader(make([]byte, 7)), size: 16}
	if _, err := r.read(0, 8); err == nil {
		t.Fatal("short header accepted")
	}
}
