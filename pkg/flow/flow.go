package flow

import (
	"sync"
	"time"
	"golang.org/x/net/context"

	//"github.com/juju/errors"
)

type SpeedControl struct {
	Rate     uint64
	Token    uint64
	MaxToken uint64
	Interval uint64 // second
	Mu       sync.Mutex
}

func NewSpeedControl(rate, token, maxToken, interval uint64) *SpeedControl {
	return &SpeedControl{
		Rate:     rate,
		Token:    token,
		MaxToken: maxToken,
		Interval: interval,
	}
}

func (f *SpeedControl)AwardToken(ctx context.Context) {
	timer := time.NewTimer(time.Duration(f.Interval)*time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			f.Mu.Lock()
			if f.Token + f.Rate*f.Interval > f.MaxToken {
				f.Token = f.MaxToken
			} else {
				f.Token += f.Rate*f.Interval
			}
			f.Mu.Unlock()
		default:
		}
	}
}

func (f *SpeedControl)ApplyToken() bool {
	f.Mu.Lock()
	defer f.Mu.Unlock()

	if f.Token > 0 {
		f.Token -= 1
		return true
	}
	
	return false
}