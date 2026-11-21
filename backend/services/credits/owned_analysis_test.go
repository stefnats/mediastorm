package credits

import (
	"image"
	"image/color"
	"math"
	"testing"
)

func ownedAnalysisFixture(text bool, brightness uint8) *image.Gray {
	out := image.NewGray(image.Rect(0, 0, 160, 90))
	for i := range out.Pix {
		out.Pix[i] = brightness
	}
	if text {
		for y := 20; y < 70; y += 10 {
			for x := 20; x < 140; x++ {
				out.SetGray(x, y, color.Gray{Y: 255})
				out.SetGray(x, y+1, color.Gray{Y: 255})
			}
		}
	}
	return out
}
func TestOwnedCreditsAnalysis(t *testing.T) {
	for _, name := range []string{"actual-brightness-fixture", "actual-edge-fixture", "coarse-transition-to-fine-range", "fine-transition-is-bounded", "plain-black-has-no-text", "invalid-or-oversized-frame-denied", "nonfinite-or-oversized-phase-data-denied"} {
		t.Run(name, func(t *testing.T) {
			switch name {
			case "actual-brightness-fixture":
				f, e := MeasureOwnedFrame(ownedAnalysisFixture(false, 120))
				if e != nil || f.Brightness != 120 || f.EdgeFraction != 0 {
					t.Fatal("actual uniform pixels", f, e)
				}
			case "actual-edge-fixture":
				f, e := MeasureOwnedFrame(ownedAnalysisFixture(true, 0))
				if e != nil || f.Brightness <= 0 || f.Brightness >= 35 || f.EdgeFraction < .005 {
					t.Fatal("actual dark text pixels", f, e)
				}
				yes, e := OwnedHasText([]FrameFeatures{f})
				if e != nil || !yes {
					t.Fatal(e)
				}
			case "coarse-transition-to-fine-range":
				bright, _ := MeasureOwnedFrame(ownedAnalysisFixture(false, 180))
				dark, _ := MeasureOwnedFrame(ownedAnalysisFixture(true, 0))
				start, yes, e := OwnedCoarseStart(200, 170, 10, []FrameFeatures{dark, dark, dark})
				if e != nil || !yes || start != 170 {
					t.Fatal(start, yes, e)
				}
				_, yes, e = OwnedCoarseStart(200, 170, 10, []FrameFeatures{bright, bright, bright})
				if e != nil || yes {
					t.Fatal("bright scene became credits", e)
				}
			case "fine-transition-is-bounded":
				frames := make([]FrameFeatures, 30)
				for i := range frames {
					frames[i].Brightness = 180
					if i >= 15 {
						frames[i].Brightness = 10
					}
				}
				start, e := OwnedFineStart(155, 1, frames)
				if e != nil || start != 170 {
					t.Fatal(start, e)
				}
				if _, e = OwnedFineStart(155, 1, append(frames, frames[0])); e == nil {
					t.Fatal("unbounded fine phase accepted")
				}
			case "plain-black-has-no-text":
				black, e := MeasureOwnedFrame(ownedAnalysisFixture(false, 0))
				if e != nil {
					t.Fatal(e)
				}
				yes, e := OwnedHasText([]FrameFeatures{black, black, black})
				if e != nil || yes {
					t.Fatal("plain black became text", e)
				}
			case "invalid-or-oversized-frame-denied":
				if _, e := MeasureOwnedFrame(nil); e == nil {
					t.Fatal("nil frame accepted")
				}
				if _, e := MeasureOwnedFrame(image.NewGray(image.Rect(0, 0, 961, 3))); e == nil {
					t.Fatal("oversized frame accepted")
				}
			case "nonfinite-or-oversized-phase-data-denied":
				if _, _, e := OwnedCoarseStart(math.Inf(1), 0, 10, []FrameFeatures{{}}); e == nil {
					t.Fatal("infinite timeline accepted")
				}
				if _, e := OwnedHasText([]FrameFeatures{{Brightness: math.NaN()}}); e == nil {
					t.Fatal("NaN image feature accepted")
				}
				if _, e := OwnedHasText(make([]FrameFeatures, 4)); e == nil {
					t.Fatal("unbounded edge phase accepted")
				}
				if _, e := OwnedFineStart(math.MaxFloat64, math.MaxFloat64, []FrameFeatures{{}, {}}); e == nil {
					t.Fatal("overflowing finite phase accepted")
				}
				if _, e := OwnedFineStart(-1, 1, []FrameFeatures{{}}); e == nil {
					t.Fatal("negative timeline accepted")
				}
			}
		})
	}
}
