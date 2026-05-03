package servicecore

// ServiceError is a local enterprise-safe error envelope for servicecore.
type ServiceError struct {
	Code    string
	Status  int
	Message string
}

func (e *ServiceError) Error() string { return e.Message }

func NewServiceError(code string, status int, message string) *ServiceError {
	return &ServiceError{Code: code, Status: status, Message: message}
}
