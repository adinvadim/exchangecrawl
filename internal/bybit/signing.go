package bybit

import (
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
)

type requestSigner struct {
	hmacSecret []byte
	rsaKey     *rsa.PrivateKey
}

func newHMACSigner(secret string) (requestSigner, error) {
	if secret == "" {
		return requestSigner{}, errors.New("Bybit API secret is empty")
	}
	return requestSigner{hmacSecret: []byte(secret)}, nil
}

func newRSASigner(privateKeyPEM []byte) (requestSigner, error) {
	block, _ := pem.Decode(privateKeyPEM)
	if block == nil {
		return requestSigner{}, errors.New("Bybit RSA private key is not valid PEM")
	}

	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return requestSigner{rsaKey: key}, nil
	}

	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return requestSigner{}, errors.New("Bybit RSA private key must be PKCS#1 or PKCS#8")
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return requestSigner{}, errors.New("Bybit PKCS#8 private key is not RSA")
	}
	return requestSigner{rsaKey: key}, nil
}

func (s requestSigner) sign(payload string) (signature string, signType string, err error) {
	if s.rsaKey != nil {
		digest := sha256.Sum256([]byte(payload))
		signed, err := rsa.SignPKCS1v15(rand.Reader, s.rsaKey, crypto.SHA256, digest[:])
		if err != nil {
			return "", "", errors.New("sign Bybit request with RSA")
		}
		return base64.StdEncoding.EncodeToString(signed), "2", nil
	}
	if len(s.hmacSecret) == 0 {
		return "", "", errors.New("Bybit request signer is not configured")
	}
	mac := hmac.New(sha256.New, s.hmacSecret)
	_, _ = mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil)), "", nil
}
