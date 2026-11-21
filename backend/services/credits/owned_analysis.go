package credits

import (
	"errors"
	"image"
	"image/color"
	"math"
)

// FrameFeatures contains measured image DATA only. It carries no source or
// process capability and cannot establish playback authority.
type FrameFeatures struct{ Brightness, EdgeFraction float64 }

var ErrOwnedAnalysis = errors.New("invalid bounded credits analysis data")

func MeasureOwnedFrame(frame image.Image) (FrameFeatures, error) {
	var out FrameFeatures
	if frame == nil {
		return out, ErrOwnedAnalysis
	}
	b := frame.Bounds()
	w, h := b.Dx(), b.Dy()
	if w < 3 || h < 3 || w > 960 || h > 960 {
		return out, ErrOwnedAnalysis
	}
	gray := make([]float64, w*h)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			c := color.GrayModel.Convert(frame.At(b.Min.X+x, b.Min.Y+y)).(color.Gray)
			gray[y*w+x] = float64(c.Y)
			out.Brightness += float64(c.Y)
		}
	}
	out.Brightness /= float64(w * h)
	edges := 0
	for y := 1; y < h-1; y++ {
		for x := 1; x < w-1; x++ {
			dx := gray[(y-1)*w+x+1] + 2*gray[y*w+x+1] + gray[(y+1)*w+x+1] - gray[(y-1)*w+x-1] - 2*gray[y*w+x-1] - gray[(y+1)*w+x-1]
			dy := gray[(y+1)*w+x-1] + 2*gray[(y+1)*w+x] + gray[(y+1)*w+x+1] - gray[(y-1)*w+x-1] - 2*gray[(y-1)*w+x] - gray[(y-1)*w+x+1]
			if math.Hypot(dx, dy) > 30 {
				edges++
			}
		}
	}
	out.EdgeFraction = float64(edges) / float64((w-2)*(h-2))
	return out, nil
}
func validOwnedFeatures(features []FrameFeatures, max int) bool {
	if len(features) == 0 || len(features) > max {
		return false
	}
	for _, f := range features {
		if math.IsNaN(f.Brightness) || math.IsInf(f.Brightness, 0) || f.Brightness < 0 || f.Brightness > 255 || math.IsNaN(f.EdgeFraction) || math.IsInf(f.EdgeFraction, 0) || f.EdgeFraction < 0 || f.EdgeFraction > 1 {
			return false
		}
	}
	return true
}

// OwnedCoarseStart follows the existing last dark-run heuristic. The caller
// must measure actual frames; unknown or incomplete samples never imply credits.
func OwnedCoarseStart(duration, start, interval float64, frames []FrameFeatures) (float64, bool, error) {
	if !validOwnedFeatures(frames, 50) || !ownedFinite(duration) || duration <= 0 || !ownedFinite(start) || start < 0 || !ownedFinite(interval) || interval <= 0 || start+float64(len(frames)-1)*interval > duration+0.001 {
		return 0, false, ErrOwnedAnalysis
	}
	first, last, gaps, dark := -1, -1, 0, 0
	for i := len(frames) - 1; i >= 0; i-- {
		if frames[i].Brightness < 35 {
			first = i
			if last < 0 {
				last = i
			}
			dark++
			gaps = 0
		} else if last >= 0 {
			gaps++
			if gaps > 2 {
				break
			}
		}
	}
	if first < 0 || last < 0 || duration-(start+float64(last)*interval) > 60 || float64(last-first+1)*interval < 20 || dark*2 < last-first+1 {
		return 0, false, nil
	}
	return start + float64(first)*interval, true, nil
}
func OwnedFineStart(start, interval float64, frames []FrameFeatures) (float64, error) {
	if !validOwnedFeatures(frames, 30) || !ownedFinite(start) || start < 0 || !ownedFinite(interval) || interval <= 0 || !ownedFinite(start+float64(len(frames)-1)*interval) {
		return 0, ErrOwnedAnalysis
	}
	first, gaps := -1, 0
	for i := len(frames) - 1; i >= 0; i-- {
		if frames[i].Brightness < 35 {
			first = i
			gaps = 0
		} else if first >= 0 {
			gaps++
			if gaps > 1 {
				break
			}
		}
	}
	if first < 0 {
		return 0, ErrOwnedAnalysis
	}
	return start + float64(first)*interval, nil
}
func OwnedHasText(frames []FrameFeatures) (bool, error) {
	if !validOwnedFeatures(frames, 3) {
		return false, ErrOwnedAnalysis
	}
	for _, f := range frames {
		if f.EdgeFraction >= 0.005 {
			return true, nil
		}
	}
	return false, nil
}
func ownedFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
