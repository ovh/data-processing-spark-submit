package upload

import (
	"io/ioutil"
	"log"
	"path"
	"path/filepath"

	"github.com/ncw/swift"

	"data-processing-spark-submit/utils"
)

type (
	Swift struct {
		StorageI
		c *swift.Connection
	}

	SwiftConf struct {
		UserName string `ini:"user_name"`
		Password string `ini:"password"`
		AuthURL  string `ini:"auth_url"`
		Domain   string `ini:"domain"`
		Region   string `ini:"region"`
	}
)

// init storage
func NewSwift(conf *SwiftConf) (*Swift, error) {
	c := &swift.Connection{
		UserName: conf.UserName,
		ApiKey:   conf.Password,
		AuthUrl:  conf.AuthURL,
		Domain:   conf.Domain,
		Region:   conf.Region,
	}
	return &Swift{
		c: c,
	}, nil
}

func (s *Swift) Upload(source, dest string) error {

	if filepath.Ext(source) != "" {
		// is dir
		if err := s.Put(source, dest); err != nil {
			return err
		}

	} else {
		files, err := ioutil.ReadDir(source)
		if err != nil {
			return err
		}

		for _, f := range files {
			if err = s.Put(path.Join(source, f.Name()), dest); err != nil {
				return err
			}
			log.Printf("File %s uploaded", filepath.Base(source))
		}
	}
	return nil
}

// Upload create/override given file(s)
func (s *Swift) Put(source, dest string) error {
	err := s.c.Authenticate()
	if err != nil {
		return err
	}

	byteFile, err := ioutil.ReadFile(source)
	if err != nil {
		return err
	}

	err = s.c.ObjectPutBytes(dest, filepath.Base(source), byteFile, utils.DetectMimeType(source))
	if err != nil {
		return err
	}
	s.c.UnAuthenticate()
	return nil
}
