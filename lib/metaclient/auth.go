// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metaclient

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"slices"
	"strconv"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"go.uber.org/zap"
	"golang.org/x/crypto/pbkdf2"
)

const (
	authCacheExpire = 3600 // Second
)

type AuthCache struct {
	mu    sync.RWMutex
	cache map[string]*UserAuthCache
}

func NewAuthCache() *AuthCache {
	return &AuthCache{
		cache: make(map[string]*UserAuthCache),
	}
}

func (ac *AuthCache) Compare(user string, pwd []byte) bool {
	ac.mu.RLock()
	cache, ok := ac.cache[user]
	ac.mu.RUnlock()

	return ok && cache.Compare(pwd)
}

func (ac *AuthCache) Create(user, base string, pwd []byte) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	cache := NewUserAuthCache(base, pwd)
	if cache != nil {
		ac.cache[user] = cache
	}
}

// CleanIfNeeded password changed or user deleted
func (ac *AuthCache) CleanIfNeeded(users map[string]string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	for name := range users {
		cache, ok := ac.cache[name]
		if ok && cache.base != users[name] {
			delete(ac.cache, name)
		}
	}

	for name := range ac.cache {
		if _, ok := users[name]; !ok {
			delete(ac.cache, name)
		}
	}
}

type UserAuthCache struct {
	base     string // used to determine whether the password is updated
	salt     []byte
	hash     []byte
	expireAt uint64
}

func NewUserAuthCache(base string, pwd []byte) *UserAuthCache {
	ac := &UserAuthCache{
		base:     base,
		salt:     make([]byte, SaltBytes),
		expireAt: fasttime.UnixTimestamp() + authCacheExpire,
	}

	_, err := rand.Read(ac.salt)
	if err != nil {
		logger.GetLogger().Error("build auth cache failed", zap.Error(err))
		return nil
	}

	ac.hash = hmacSha256(ac.salt, pwd)
	return ac
}

func (ac *UserAuthCache) Compare(pwd []byte) bool {
	if fasttime.UnixTimestamp() > ac.expireAt {
		// expired
		return false
	}

	hash := hmacSha256(ac.salt, pwd)
	return slices.Equal(hash, ac.hash)
}

func hmacSha256(key, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	return hash.Sum(data)
}

type AuthFailedLog struct {
	lastTime uint64
	count    uint64
}

func (log *AuthFailedLog) Add() {
	if (log.lastTime + lockUserTime) < fasttime.UnixTimestamp() {
		log.count = 0
	}

	log.count++
	log.lastTime = fasttime.UnixTimestamp()
}

func (log *AuthFailedLog) Locked() bool {
	return log.count >= maxLoginLimit &&
		(log.lastTime+lockUserTime) > fasttime.UnixTimestamp()
}

type AuthFailed struct {
	mu   sync.RWMutex
	logs map[string]*AuthFailedLog
}

func (af *AuthFailed) Add(user string) {
	af.mu.Lock()
	defer af.mu.Unlock()

	log, ok := af.logs[user]
	if !ok {
		log = &AuthFailedLog{}
		af.logs[user] = log
	}
	log.Add()
}

func (af *AuthFailed) Clean(user string) {
	af.mu.Lock()
	defer af.mu.Unlock()

	delete(af.logs, user)
}

func (af *AuthFailed) Locked(user string) bool {
	af.mu.RLock()
	defer af.mu.RUnlock()

	log, ok := af.logs[user]
	return ok && log.Locked()
}

type Auth struct {
	logger *logger.Logger

	cache  *AuthCache
	failed *AuthFailed

	optAlgoVer int
}

func NewAuth(logger *logger.Logger) *Auth {
	return &Auth{
		logger: logger,
		cache:  NewAuthCache(),
		failed: &AuthFailed{
			logs: make(map[string]*AuthFailedLog),
		},
	}
}

func (a *Auth) SetHashAlgo(optHashAlgo string) {
	a.optAlgoVer = cvtDataForAlgoVer(optHashAlgo)
}

// hashWithSalt returns a salted hash of password using salt.
func (a *Auth) hashWithSalt(salt []byte, password string) []byte {
	hasher := sha256.New()
	buf := make([]byte, 0, len(salt)+len(password))
	buf = append(buf, salt...)
	buf = append(buf, password...)
	return hasher.Sum(buf)
}

// saltedHash returns a salt and salted hash of password.
func (a *Auth) saltedHash(password string) (salt, hash []byte, err error) {
	salt = make([]byte, SaltBytes)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, nil, err
	}

	return salt, a.hashWithSalt(salt, password), nil
}

func (a *Auth) genHashPwdVal(password string) (string, error) {
	switch a.optAlgoVer {
	case algoVer01:
		return a.genSHA256PwdVal(password)
	default:
		return a.GenPbkdf2PwdVal(password)
	}
}

// generates the salted hash value of the password (using SHA256).
func (a *Auth) genSHA256PwdVal(password string) (string, error) {
	// 1.generate a salt and hash of the password for the cache
	salt, hashed, err := a.saltedHash(password)
	if err != nil {
		logger.GetLogger().Error("saltedHash fail", zap.Error(err))
		return "", err
	}

	// 2.assemble, verFlag + salt + hashedVal
	rstVal := hashAlgoVerOne
	rstVal += fmt.Sprintf("%02X", salt)   // convert to  hex string
	rstVal += fmt.Sprintf("%02X", hashed) // convert to hex string
	return rstVal, nil
}

// pbkdf2WithSalt returns an encryption of password using salt.
func (a *Auth) pbkdf2WithSalt(salt []byte, password string, algoVer int) []byte {
	pbkdf2Iter := pbkdf2Iter4096
	if algoVer == algoVer03 {
		pbkdf2Iter = pbkdf2Iter1000
	}
	dk := pbkdf2.Key([]byte(password), salt, pbkdf2Iter, pbkdf2KeyLen, sha256.New)
	return dk
}

// saltedPbkdf2 returns a salt and used pbkdf2 of password.
func (a *Auth) saltedPbkdf2(password string, algoVer int) (salt, hash []byte, err error) {
	salt = make([]byte, SaltBytes)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, nil, err
	}

	return salt, a.pbkdf2WithSalt(salt, password, algoVer), nil
}

// GenPbkdf2PwdVal generates the salted hash value of the password (using PBKDF2)
func (a *Auth) GenPbkdf2PwdVal(password string) (string, error) {
	// step 1. generate a salt and hash of the password for the cache
	salt, hashed, err := a.saltedPbkdf2(password, a.optAlgoVer)
	if err != nil {
		logger.GetLogger().Error("saltedHash fail", zap.Error(err))
		return "", err
	}

	// step 2. assemble (verFlag + salt + hashedVal(PBKDF2))
	var rstVal string
	switch a.optAlgoVer {
	case algoVer02:
		rstVal = hashAlgoVerTwo
	case algoVer03:
		rstVal = hashAlgoVerThree
	default:
		rstVal = hashAlgoVerTwo
	}
	rstVal += fmt.Sprintf("%02X", salt)   // convert to  hex string
	rstVal += fmt.Sprintf("%02X", hashed) // convert to hex string
	return rstVal, nil
}

// for hash ver One
func (a *Auth) compareHashAndPwdVerOne(hashed, plaintext string) error {
	if len(hashed) < len(hashAlgoVerOne)+SaltBytes*2 {
		return meta.ErrHashedLength
	}
	verFlagLen := len(hashAlgoVerOne)
	saltStr := hashed[verFlagLen : verFlagLen+SaltBytes*2]
	hashStr := hashed[verFlagLen+SaltBytes*2:]

	salt, err := func(s string) ([]byte, error) {
		rstSlice := make([]byte, len(s)/2)
		for i := 0; i+1 < len(s); {
			uVal, err := strconv.ParseUint(s[i:i+2], 16, 8) //16 base, 8 bitSize
			if err != nil {
				logger.GetLogger().Error("hash pwd VerOne convert str to uint fail", zap.Error(err))
				return nil, err
			}
			rstSlice[i/2] = byte(uVal & 0xFF)
			i += 2
		}
		return rstSlice, nil
	}(saltStr)

	if err != nil {
		return err
	}

	// gen hasded strVal from given plain pwd
	newHashStr := fmt.Sprintf("%02X", a.hashWithSalt(salt, plaintext))

	if hashStr != newHashStr {
		return meta.ErrMismatchedHashAndPwd
	}
	return nil
}

// for hash ver Two
func (a *Auth) compareHashAndPwdVerTwo(hashed, plaintext string, algoVer int) error {
	if len(hashed) < len(hashAlgoVerTwo)+SaltBytes*2 {
		return meta.ErrHashedLength
	}
	verFlagLen := len(hashAlgoVerTwo)
	saltStr := hashed[verFlagLen : verFlagLen+SaltBytes*2]
	hashStr := hashed[verFlagLen+SaltBytes*2:]

	salt, err := func(s string) ([]byte, error) {
		rstSlice := make([]byte, len(s)/2)
		for i := 0; i+1 < len(s); {
			uVal, err := strconv.ParseUint(s[i:i+2], 16, 8) //16 base, 8 bitSize
			if err != nil {
				logger.GetLogger().Error("hash pwd VerTwo convert str to uint fail", zap.Error(err))
				return nil, err
			}
			rstSlice[i/2] = byte(uVal & 0xFF)
			i += 2
		}
		return rstSlice, nil
	}(saltStr)

	if err != nil {
		return err
	}

	// gen pbkdf2 hashed strVal from given plain pwd
	dk := a.pbkdf2WithSalt(salt, plaintext, algoVer)
	newHashStr := fmt.Sprintf("%02X", dk)

	if hashStr != newHashStr {
		return meta.ErrMismatchedHashAndPwd
	}
	return nil
}

// compares a hashed password with its possible
// plaintext equivalent. Returns nil on success, or an error on failure.
func (a *Auth) CompareHashAndPlainPwd(hashed, plaintext string) error {
	if len(hashed) < len(hashAlgoVerOne) {
		return meta.ErrHashedLength
	}

	hashVer := hashed[:len(hashAlgoVerOne)]
	switch hashVer {
	case hashAlgoVerOne:
		return a.compareHashAndPwdVerOne(hashed, plaintext)
	case hashAlgoVerTwo:
		return a.compareHashAndPwdVerTwo(hashed, plaintext, algoVer02)
	case hashAlgoVerThree:
		return a.compareHashAndPwdVerTwo(hashed, plaintext, algoVer03)

	default:
		return meta.ErrMismatchedHashAndPwd
	}
}

func (a *Auth) Authenticate(user *meta.UserInfo, password string) error {
	err := a.authenticate(user, password)
	if err == nil {
		a.failed.Clean(user.Name)
	} else {
		a.failed.Add(user.Name)
	}
	return err
}

func (a *Auth) authenticate(user *meta.UserInfo, password string) error {
	pwd := util.Str2bytes(password)

	// Check the local auth cache first.
	if a.cache.Compare(user.Name, pwd) {
		return nil
	}

	// Compare password with user hash.
	if err := a.CompareHashAndPlainPwd(user.Hash, password); err != nil {
		return meta.ErrAuthenticate
	}

	a.cache.Create(user.Name, user.Hash, pwd)
	return nil
}

func (a *Auth) UpdateAuthCache(users []meta.UserInfo) {
	userMap := make(map[string]string, len(users))
	for _, user := range users {
		userMap[user.Name] = user.Hash
	}
	a.cache.CleanIfNeeded(userMap)
}

func (a *Auth) IsLockedUser(user string) bool {
	return a.failed.Locked(user)
}
