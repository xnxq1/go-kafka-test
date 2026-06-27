package domain

type Status string

const (
	StatusPending Status = "pending"
	StatusSent    Status = "sent"
	StatusFailed  Status = "failed"
)

// String возвращает строковое представление статуса.
func (s Status) String() string {
	return string(s)
}

// IsValid сообщает, является ли статус одним из допустимых значений.
func (s Status) IsValid() bool {
	switch s {
	case StatusPending, StatusSent, StatusFailed:
		return true
	default:
		return false
	}
}
