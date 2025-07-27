package service

import "errors"

var (
	ErrUnprocessableEntity = errors.New("unprocessable entity")
	ErrInternalServerError = errors.New("internal server error")
)
