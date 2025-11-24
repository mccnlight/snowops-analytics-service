package model

import "github.com/google/uuid"

type UserRole string

const (
	UserRoleAkimatAdmin     UserRole = "AKIMAT_ADMIN"
	UserRoleKguZkhAdmin     UserRole = "KGU_ZKH_ADMIN"
	UserRoleTooAdmin        UserRole = "TOO_ADMIN" // Deprecated: use LANDFILL_ADMIN
	UserRoleLandfillAdmin   UserRole = "LANDFILL_ADMIN"
	UserRoleLandfillUser    UserRole = "LANDFILL_USER"
	UserRoleContractorAdmin UserRole = "CONTRACTOR_ADMIN"
	UserRoleDriver          UserRole = "DRIVER"
)

type Principal struct {
	UserID   uuid.UUID
	OrgID    uuid.UUID
	Role     UserRole
	DriverID *uuid.UUID
}

func (p Principal) IsAkimat() bool {
	return p.Role == UserRoleAkimatAdmin
}

func (p Principal) IsKgu() bool {
	return p.Role == UserRoleKguZkhAdmin
}

func (p Principal) IsToo() bool {
	return p.Role == UserRoleTooAdmin
}

// IsLandfill проверяет, является ли пользователь администратором или пользователем полигона
// Также поддерживает обратную совместимость с TOO_ADMIN
func (p Principal) IsLandfill() bool {
	return p.Role == UserRoleLandfillAdmin || p.Role == UserRoleLandfillUser || p.Role == UserRoleTooAdmin
}

func (p Principal) IsContractor() bool {
	return p.Role == UserRoleContractorAdmin
}

func (p Principal) IsDriver() bool {
	return p.Role == UserRoleDriver
}
