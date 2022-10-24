package upload

import (
	"fmt"

	ini "gopkg.in/ini.v1"
)

type (
	StorageI interface {
		Upload(source, dest string) error
	}
)

func New(section *ini.Section, protocol string) (StorageI, error) {

	switch protocol {
	case "swift":
		s := new(SwiftConf)
		if err := section.MapTo(s); err != nil {
			return nil, err
		}
		return NewSwift(s)
	default:
		return nil, fmt.Errorf("%s protocol not implemented yet", protocol)
	}

}
