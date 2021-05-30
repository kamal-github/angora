package backoff

import "time"

// ExponentialWait implements exponential backoff and sleeps for that duration
// as long as maxBackOff is reached.
func ExponentialWait(retried, maxBackOff int) {
	delay := min((2^retried)+5, maxBackOff)

	time.Sleep(time.Duration(delay) * time.Second)
}

// min finds minimum of 2 int.
func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}
