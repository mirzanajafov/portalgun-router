package httpx

import (
	"crypto/subtle"
	"net/http"
)

func Authorized(r *http.Request, token string) bool {
	return token != "" && Carries(r, "Authorization", "Bearer "+token)
}

func Carries(r *http.Request, header, secret string) bool {
	return secret != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get(header)), []byte(secret)) == 1
}
