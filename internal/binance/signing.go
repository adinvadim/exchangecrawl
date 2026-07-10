package binance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

func sign(secret, query string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(query))
	return hex.EncodeToString(mac.Sum(nil))
}
