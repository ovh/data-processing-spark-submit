package upload

import (
	"testing"

	"github.com/ncw/swift"
	"github.com/ncw/swift/swifttest"
)

type testObject struct {
	Container string
	Path      string
	Content   string
}

var testContainer = "test"

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func TestNewSwift2(t *testing.T) {
	conf := &SwiftConf{
		UserName: "UserName",
		Password: "Password",
		AuthURL:  "AuthURL",
		Domain:   "Domain",
		Region:   "Region",
	}

	s, _ := NewSwift(conf)

	if s.c.UserName != conf.UserName {
		t.Fail()
	}
	if s.c.ApiKey != conf.Password {
		t.Fail()
	}
	if s.c.AuthUrl != conf.AuthURL {
		t.Fail()
	}
	if s.c.Domain != conf.Domain {
		t.Fail()
	}
	if s.c.Region != conf.Region {
		t.Fail()
	}
}

func TestSwiftPutFile(t *testing.T) {
	srv, err := swifttest.NewSwiftServer("localhost")
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			srv.Close()
		}
	}()
	client := &swift.Connection{
		UserName: swifttest.TEST_ACCOUNT,
		ApiKey:   swifttest.TEST_ACCOUNT,
		AuthUrl:  srv.AuthURL,
	}
	client.Authenticate()

	if err = client.ContainerCreate(testContainer, nil); err != nil {
		return
	}
	if err != nil && t != nil {
		t.Fatal("Failed to create server", err)
	}
	client.UnAuthenticate()

	conf := &SwiftConf{
		UserName: swifttest.TEST_ACCOUNT,
		Password: swifttest.TEST_ACCOUNT,
		AuthURL:  srv.AuthURL,
	}

	s, _ := NewSwift(conf)

	err = s.Upload("../testdata/jar.jar", testContainer)
	if err != nil {
		t.Fail()
	}

	res, err := client.ObjectNames(testContainer, nil)
	if err != nil {
		t.Error()
	}

	if !stringInSlice("jar.jar", res) {
		t.Fail()
	}

}

func TestSwiftPutFiles(t *testing.T) {
	srv, err := swifttest.NewSwiftServer("localhost")
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			srv.Close()
		}
	}()
	client := &swift.Connection{
		UserName: swifttest.TEST_ACCOUNT,
		ApiKey:   swifttest.TEST_ACCOUNT,
		AuthUrl:  srv.AuthURL,
	}
	client.Authenticate()

	if err = client.ContainerCreate(testContainer, nil); err != nil {
		return
	}
	if err != nil && t != nil {
		t.Fatal("Failed to create server", err)
	}
	client.UnAuthenticate()

	conf := &SwiftConf{
		UserName: swifttest.TEST_ACCOUNT,
		Password: swifttest.TEST_ACCOUNT,
		AuthURL:  srv.AuthURL,
	}

	s, _ := NewSwift(conf)

	err = s.Upload("../testdata/", testContainer)
	if err != nil {
		t.Fail()
	}

	res, err := client.ObjectNames(testContainer, nil)
	if err != nil {
		t.Error()
	}

	if !stringInSlice("jar.jar", res) {
		t.Fail()
	}

	if !stringInSlice("py.py", res) {
		t.Fail()
	}

	if !stringInSlice("py2.py", res) {
		t.Fail()
	}

	if !stringInSlice("configuration.ini", res) {
		t.Fail()
	}

}
