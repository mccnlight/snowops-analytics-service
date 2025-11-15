package model

import "github.com/google/uuid"

type ScopeType string

const (
	ScopeCity       ScopeType = "CITY"
	ScopeKgu        ScopeType = "KGU"
	ScopeContractor ScopeType = "CONTRACTOR"
	ScopeTechnical  ScopeType = "TECHNICAL"
)

type Scope struct {
	Type               ScopeType
	OrgID              *uuid.UUID
	OrganizationIDs    []uuid.UUID
	ContractorIDs      []uuid.UUID
	IncludeContractors bool
	TechnicalOnly      bool
}

func (s Scope) AllowsCity() bool {
	return s.Type == ScopeCity
}

func (s Scope) AllowsKgu() bool {
	return s.Type == ScopeKgu || s.Type == ScopeCity
}

func (s Scope) AllowsContractor(contractorID uuid.UUID) bool {
	if s.Type == ScopeCity {
		return true
	}
	if s.Type == ScopeContractor && s.OrgID != nil {
		return *s.OrgID == contractorID
	}
	for _, id := range s.ContractorIDs {
		if id == contractorID {
			return true
		}
	}
	return false
}
