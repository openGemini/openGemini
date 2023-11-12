package sftpclient

import (
	"github.com/golang/mock/gomock"
	"github.com/openGemini/openGemini/lib/mocks"
	"github.com/pkg/sftp"
	"os"
	"path"
	"strings"
	"testing"
)

func TestClient_Backup(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockSFTPClient := mocks.NewMockSFTPClient(mockCtrl)

	localPath := path.Join(t.TempDir(), "source")
	sourceFile := path.Join(localPath, "db0", "1.tssp")
	if err := os.MkdirAll(path.Dir(sourceFile), 0755); err != nil {
		t.Fatalf("create dir error:%v", err)
	}
	if _, err := os.Create(sourceFile); err != nil {
		t.Fatalf("create file error:%v", err)
	}

	remotePath := path.Join(t.TempDir(), "dest")
	destFile := strings.Replace(sourceFile, localPath, remotePath, 1)
	mockSFTPClient.EXPECT().MkdirAll(path.Dir(destFile)).Return(os.MkdirAll(path.Dir(destFile), 0755))
	file := &sftp.File{}
	mockSFTPClient.EXPECT().Create(destFile).Return(file, nil)

	client := &Client{SFTPClient(mockSFTPClient)}
	if err := client.Copy(sourceFile, destFile); err != nil {
		t.Fatalf("backup error:%v", err)
	}

}
