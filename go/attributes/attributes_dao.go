/*
   Copyright 2014 Outbrain Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package attributes

import (
	"fmt"
	"strings"

	"github.com/proxysql/golib/log"
	"github.com/proxysql/golib/sqlutils"
	"github.com/proxysql/orchestrator/go/db"
)

// Store gives access to the host_attributes table of ONE tenant backend.
// host_attributes is tenant-scoped data: every tenant has its own table in its
// own backend.
type Store struct {
	backend *db.Backend
}

// NewStore returns the attributes store of the given tenant backend.
func NewStore(backend *db.Backend) *Store {
	return &Store{backend: backend}
}

// SetHostAttributes
func (s *Store) SetHostAttributes(hostname string, attributeName string, attributeValue string) error {
	_, err := s.backend.Exec(`
			replace
				into host_attributes (
					hostname, attribute_name, attribute_value, submit_timestamp, expire_timestamp
				) VALUES (
					?, ?, ?, NOW(), NULL
				)
			`,
		hostname,
		attributeName,
		attributeValue,
	)
	if err != nil {
		return log.Errore(err)
	}

	return err
}

func (s *Store) getHostAttributesByClause(whereClause string, args []interface{}) ([]HostAttributes, error) {
	res := []HostAttributes{}
	query := fmt.Sprintf(`
		select
			hostname,
			attribute_name,
			attribute_value,
			submit_timestamp ,
			ifnull(expire_timestamp, '') as expire_timestamp
		from
			host_attributes
		%s
		order by
			hostname, attribute_name
		`, whereClause)

	err := s.backend.Query(query, args, func(m sqlutils.RowMap) error {
		hostAttributes := HostAttributes{}
		hostAttributes.Hostname = m.GetString("hostname")
		hostAttributes.AttributeName = m.GetString("attribute_name")
		hostAttributes.AttributeValue = m.GetString("attribute_value")
		hostAttributes.SubmitTimestamp = m.GetString("submit_timestamp")
		hostAttributes.ExpireTimestamp = m.GetString("expire_timestamp")

		res = append(res, hostAttributes)
		return nil
	})

	if err != nil {
		_ = log.Errore(err)
	}
	return res, err
}

// GetHostAttributesByMatch
func (s *Store) GetHostAttributesByMatch(hostnameMatch string, attributeNameMatch string, attributeValueMatch string) ([]HostAttributes, error) {
	terms := []string{}
	args := sqlutils.Args()
	if hostnameMatch != "" {
		terms = append(terms, ` hostname rlike ? `)
		args = append(args, hostnameMatch)
	}
	if attributeNameMatch != "" {
		terms = append(terms, ` attribute_name rlike ? `)
		args = append(args, attributeNameMatch)
	}
	if attributeValueMatch != "" {
		terms = append(terms, ` attribute_value rlike ? `)
		args = append(args, attributeValueMatch)
	}

	if len(terms) == 0 {
		return s.getHostAttributesByClause("", args)
	}
	whereCondition := fmt.Sprintf(" where %s ", strings.Join(terms, " and "))

	return s.getHostAttributesByClause(whereCondition, args)
}

// GetHostAttribute expects to return a single attribute for a given hostname/attribute-name combination
// or error on empty result
func (s *Store) GetHostAttribute(hostname string, attributeName string) (string, error) {
	whereClause := `where hostname=? and attribute_name=?`
	attributes, err := s.getHostAttributesByClause(whereClause, sqlutils.Args(hostname, attributeName))
	if err != nil {
		return "", err
	}
	if len(attributeName) == 0 {
		return "", log.Errorf("No attribute found for %+v, %+v", hostname, attributeName)
	}
	return attributes[0].AttributeValue, nil
}

// SetGeneralAttribute sets an attribute not associated with a host. Its a key-value thing
func (s *Store) SetGeneralAttribute(attributeName string, attributeValue string) error {
	if attributeName == "" {
		return nil
	}
	return s.SetHostAttributes("*", attributeName, attributeValue)
}

// GetGeneralAttribute expects to return a single attribute value (not associated with a specific hostname)
func (s *Store) GetGeneralAttribute(attributeName string) (result string, err error) {
	return s.GetHostAttribute("*", attributeName)
}

// GetHostAttributesByAttribute
func (s *Store) GetHostAttributesByAttribute(attributeName string, valueMatch string) ([]HostAttributes, error) {
	if valueMatch == "" {
		valueMatch = ".?"
	}
	whereClause := ` where attribute_name = ? and attribute_value rlike ?`

	return s.getHostAttributesByClause(whereClause, sqlutils.Args(attributeName, valueMatch))
}

// DefaultStore returns the store of the legacy single-tenant backend
// (db.DefaultBackend()).
func DefaultStore() *Store {
	return NewStore(db.DefaultBackend())
}

// Deprecated: package-level wrappers over DefaultStore(), kept so callers that
// are not tenant-aware yet keep compiling. Use a Store.

// SetHostAttributes is the legacy wrapper of (*Store).SetHostAttributes.
func SetHostAttributes(hostname string, attributeName string, attributeValue string) error {
	return DefaultStore().SetHostAttributes(hostname, attributeName, attributeValue)
}

// GetHostAttributesByMatch is the legacy wrapper of (*Store).GetHostAttributesByMatch.
func GetHostAttributesByMatch(hostnameMatch string, attributeNameMatch string, attributeValueMatch string) ([]HostAttributes, error) {
	return DefaultStore().GetHostAttributesByMatch(hostnameMatch, attributeNameMatch, attributeValueMatch)
}

// GetHostAttribute is the legacy wrapper of (*Store).GetHostAttribute.
func GetHostAttribute(hostname string, attributeName string) (string, error) {
	return DefaultStore().GetHostAttribute(hostname, attributeName)
}

// SetGeneralAttribute is the legacy wrapper of (*Store).SetGeneralAttribute.
func SetGeneralAttribute(attributeName string, attributeValue string) error {
	return DefaultStore().SetGeneralAttribute(attributeName, attributeValue)
}

// GetGeneralAttribute is the legacy wrapper of (*Store).GetGeneralAttribute.
func GetGeneralAttribute(attributeName string) (result string, err error) {
	return DefaultStore().GetGeneralAttribute(attributeName)
}

// GetHostAttributesByAttribute is the legacy wrapper of (*Store).GetHostAttributesByAttribute.
func GetHostAttributesByAttribute(attributeName string, valueMatch string) ([]HostAttributes, error) {
	return DefaultStore().GetHostAttributesByAttribute(attributeName, valueMatch)
}
