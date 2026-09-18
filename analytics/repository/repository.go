package repository

import "analytics-service/analytics"

type Repository interface {
	Save(results []analytics.Result) error
}
