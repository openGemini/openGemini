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

package metaclient_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/require"
)

func TestAuthCacheCompare(t *testing.T) {
	ac := metaclient.NewAuthCache()
	require.NotNil(t, ac, "NewAuthCache should not return nil")

	ac.Create("user1", "base1", []byte("pwd1"))

	// Test with correct password
	require.True(t, ac.Compare("user1", []byte("pwd1")), "Compare should return true for correct password")

	// Test with incorrect password
	require.False(t, ac.Compare("user1", []byte("wrongpwd")), "Compare should return false for incorrect password")

	// Test with non-existing user
	require.False(t, ac.Compare("user2", []byte("pwd1")), "Compare should return false for non-existing user")
}

func TestAuthCacheCleanIfNeeded(t *testing.T) {
	ac := metaclient.NewAuthCache()
	ac.Create("user1", "base1", []byte("pwd1"))

	// Test with same base
	ac.CleanIfNeeded(map[string]string{"user1": "base1"})
	require.True(t, ac.Compare("user1", []byte("pwd1")), "User should not be cleaned")

	// Test with different base
	ac.CleanIfNeeded(map[string]string{"user1": "base2"})
	require.False(t, ac.Compare("user1", []byte("pwd1")), "User should be cleaned")

	// Test with non-existing user
	ac.CleanIfNeeded(map[string]string{"user2": "base1"})
	require.False(t, ac.Compare("user2", []byte("pwd1")), "Non-existing user should not be cleaned")
}

func TestAuthenticate(t *testing.T) {
	auth := metaclient.NewAuth(logger.NewLogger(errno.ModuleMetaClient))
	auth.SetHashAlgo("ver02")

	hash, err := auth.GenPbkdf2PwdVal("testPassword")
	require.NoError(t, err)

	user := &meta.UserInfo{
		Name: "testUser",
		Hash: hash,
	}

	var assertAuthSuccess = func(pwd string) {
		err = auth.Authenticate(user, pwd)
		require.NoError(t, err)
	}

	var assertAuthFailed = func(pwd string) {
		err = auth.Authenticate(user, pwd)
		require.Error(t, err)
		require.Equal(t, meta.ErrAuthenticate, err)
	}

	assertAuthSuccess("testPassword")
	assertAuthSuccess("testPassword") // form cache

	// change password
	hash, err = auth.GenPbkdf2PwdVal("testPassword_01")
	require.NoError(t, err)
	user.Hash = hash
	auth.UpdateAuthCache([]meta.UserInfo{*user})

	assertAuthFailed("testPassword") // old password
	assertAuthSuccess("testPassword_01")

	for range 3 {
		assertAuthFailed("wrongPassword")
	}
	require.False(t, auth.IsLockedUser(user.Name))
	assertAuthSuccess("testPassword_01") // clean auth failed log

	for range 10 {
		assertAuthFailed("wrongPassword")
	}
	require.True(t, auth.IsLockedUser(user.Name))
}

func TestClient_Authenticate(t *testing.T) {
	cli := metaclient.NewClient("", false, 10)
	cli.SetCacheData(&meta.Data{Users: []meta.UserInfo{
		{
			Name: "name",
			Hash: "",
		},
	}})
	_, err := cli.Authenticate("name", "abc@123AAA")
	require.Error(t, err)
}

func TestCompareHashAndPlainPwd(t *testing.T) {
	auth := metaclient.NewAuth(logger.NewLogger(errno.ModuleMetaClient))

	require.Error(t, auth.CompareHashAndPlainPwd("aa", "password"))
	require.Error(t, auth.CompareHashAndPlainPwd("#Ver:001#aa", "password"))
	require.Error(t, auth.CompareHashAndPlainPwd("#Ver:002#aa", "password"))
	require.Error(t, auth.CompareHashAndPlainPwd("#Ver:003#aa", "password"))
	require.Error(t, auth.CompareHashAndPlainPwd("#Ver:009#aa", "password"))

	hash := string(make([]byte, 128))
	require.Error(t, auth.CompareHashAndPlainPwd("#Ver:001#"+hash, "password"))
	require.Error(t, auth.CompareHashAndPlainPwd("#Ver:002#"+hash, "password"))
	require.Error(t, auth.CompareHashAndPlainPwd("#Ver:003#"+hash, "password"))
}
