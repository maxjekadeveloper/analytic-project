package cache

import (
	"analytics-service/cache"
	"analytics-service/statistics"
	"context"
	"encoding/json"
	"log"
	"time"
)

type AnalyticsRepository interface {
	GetStats(ctx context.Context) (statistics.Statistics, error)
}

type CacheService struct {
	repo  AnalyticsRepository
	cache *cache.RedisCache
}

func NewCacheService(repo AnalyticsRepository, cache *cache.RedisCache) *CacheService {
	return &CacheService{repo: repo, cache: cache}
}

func (c *CacheService) GetStats(ctx context.Context) (statistics.Statistics, error) {
	const key = "analytics:stats"
	const ttl = time.Minute

	// 1. Проверяем Redis
	value, err := c.cache.Get(ctx, key)
	if err == nil {
		var stats statistics.Statistics
		err = json.Unmarshal([]byte(value), &stats)
		if err == nil {
			log.Println("Cache HIT")
			return stats, nil
		}
	}

	log.Println("Cache MISS")

	// 2. Получаем данные из PostgreSQL
	stats, err := c.repo.GetStats(ctx)
	if err != nil {
		return statistics.Statistics{}, err
	}

	// 3. Сериализуем структуру
	data, err := json.Marshal(stats)
	if err != nil {
		return statistics.Statistics{}, err
	}

	// 4. Записываем результат в Redis
	err = c.cache.Set(ctx, key, string(data), ttl)
	if err != nil {
		log.Printf("Redis SET failed: $v\n", err)
	}

	return stats, nil
}
