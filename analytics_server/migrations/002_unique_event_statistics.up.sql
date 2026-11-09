ALTER TABLE event_statistics
ADD CONSTRAINT event_statistics_window_event_unique
UNIQUE (window_start, window_end, event_type);