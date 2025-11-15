package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"analytics-service/internal/model"
)

const (
	orgTypeContractor = "CONTRACTOR"
)

type ScopeRepository struct {
	db *gorm.DB
}

var ErrScopeUnsupported = errors.New("principal role is not allowed in analytics")

func NewScopeRepository(db *gorm.DB) *ScopeRepository {
	return &ScopeRepository{db: db}
}

func (r *ScopeRepository) ResolveScope(ctx context.Context, principal model.Principal) (model.Scope, error) {
	scope := model.Scope{}

	switch {
	case principal.IsAkimat():
		scope.Type = model.ScopeCity
		return scope, nil
	case principal.IsKgu():
		scope.Type = model.ScopeKgu
		scope.OrgID = &principal.OrgID
		contractors, err := r.listContractors(ctx, principal.OrgID)
		if err != nil {
			return model.Scope{}, err
		}
		scope.ContractorIDs = contractors
		scope.OrganizationIDs = append([]uuid.UUID{principal.OrgID}, contractors...)
		scope.IncludeContractors = true
		return scope, nil
	case principal.IsContractor():
		scope.Type = model.ScopeContractor
		scope.OrgID = &principal.OrgID
		scope.ContractorIDs = []uuid.UUID{principal.OrgID}
		scope.OrganizationIDs = []uuid.UUID{principal.OrgID}
		return scope, nil
	case principal.IsToo():
		scope.Type = model.ScopeTechnical
		scope.TechnicalOnly = true
		return scope, nil
	default:
		return model.Scope{}, ErrScopeUnsupported
	}
}

func (r *ScopeRepository) listContractors(ctx context.Context, parent uuid.UUID) ([]uuid.UUID, error) {
	rows := make([]uuid.UUID, 0)
	type result struct {
		ID uuid.UUID
	}
	var data []result
	if err := r.db.WithContext(ctx).
		Table("organizations").
		Select("id").
		Where("parent_org_id = ? AND type = ? AND is_active = ?", parent, orgTypeContractor, true).
		Find(&data).Error; err != nil {
		return nil, err
	}
	for _, row := range data {
		rows = append(rows, row.ID)
	}
	return rows, nil
}
