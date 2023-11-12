package sftpclient

import (
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"net"
	"os"
	"path"
	"time"
)

type SFTPClient interface {
	Open(path string) (*sftp.File, error)
	MkdirAll(path string) error
	Create(path string) (*sftp.File, error)
}

type Client struct {
	SFTPClient
}

func NewClient(addr string, user string, auth []ssh.AuthMethod) (*Client, error) {
	sshClient, err := NewSSHClient(addr, user, auth)
	if err != nil {
		return nil, err
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, err
	}

	return &Client{SFTPClient: sftpClient}, nil
}

func NewSSHClient(addr string, user string, auth []ssh.AuthMethod) (*ssh.Client, error) {
	clientConfig := &ssh.ClientConfig{
		User:    user,
		Auth:    auth,
		Timeout: 30 * time.Second,
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}

	sshClient, err := ssh.Dial("tcp", addr, clientConfig)
	if err != nil {
		return nil, err
	}

	return sshClient, nil
}

func (c *Client) Copy(localDataPath, remotePath string) error {
	source, err := os.Open(localDataPath)
	if err != nil {
		return err
	}
	defer source.Close()

	stat, err := source.Stat()
	if err != nil {
		return err
	}

	if stat.IsDir() {
		err = c.MkdirAll(remotePath)
		return nil
	}

	if err = c.MkdirAll(path.Dir(remotePath)); err != nil {
		return err
	}

	dest, err := c.Create(path.Join(remotePath))
	if err != nil {
		return err
	}

	_, err = io.Copy(dest, source)
	return err
}
