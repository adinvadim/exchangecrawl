package bybit

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"
)

func TestHMACSignatureMatchesBybitBarGoldenVector(t *testing.T) {
	t.Parallel()

	signer, err := newHMACSigner("test-secret")
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}

	signature, signType, err := signer.sign("1700000000000test-api-key5000category=linear&symbol=BTCUSDT")
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if want := "184dd5531d8077974f3a400e537b3ec9b117b9915683f889177fe56e8a97f4e9"; signature != want {
		t.Fatalf("signature = %q, want %q", signature, want)
	}
	if signType != "" {
		t.Fatalf("sign type = %q, want empty", signType)
	}
}

func TestRSASignatureSupportsPKCS1AndPKCS8PEM(t *testing.T) {
	t.Parallel()

	privateKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	payload := "1700000000000test-api-key5000category=linear&symbol=BTCUSDT"
	digest := sha256.Sum256([]byte(payload))

	tests := map[string][]byte{
		"PKCS1": pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
		}),
		"PKCS8": mustMarshalPKCS8(t, privateKey),
	}

	for name, encoded := range tests {
		encoded := encoded
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			signer, err := newRSASigner(encoded)
			if err != nil {
				t.Fatalf("new signer: %v", err)
			}
			signature, signType, err := signer.sign(payload)
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			if signType != "2" {
				t.Fatalf("sign type = %q, want 2", signType)
			}
			raw, err := base64.StdEncoding.DecodeString(signature)
			if err != nil {
				t.Fatalf("decode signature: %v", err)
			}
			if err := rsa.VerifyPKCS1v15(&privateKey.PublicKey, crypto.SHA256, digest[:], raw); err != nil {
				t.Fatalf("verify signature: %v", err)
			}
		})
	}
}

func mustMarshalPKCS8(t *testing.T, privateKey *rsa.PrivateKey) []byte {
	t.Helper()

	encoded, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal PKCS8: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
}
