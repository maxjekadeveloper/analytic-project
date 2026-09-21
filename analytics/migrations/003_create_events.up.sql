CREATE TABLE IF NOT EXISTS events (
  event_id UUID PRIMARY KEY,
  event_type TEXT NOT NULL,
  element_id TEXT NOT NULL,
  value JSONB,
  event_timestamp TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)