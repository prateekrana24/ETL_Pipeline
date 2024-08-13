CREATE INDEX idx_datetime ON tsla_daily_data(datetime);
CREATE INDEX idx_close ON tsla_daily_data(close);
CREATE INDEX idx_open ON tsla_daily_data(open);
CREATE INDEX idx_volume ON tsla_daily_data(volume);

DROP INDEX idx_datetime ON tsla_daily_data;
DROP INDEX idx_close ON tsla_daily_data;
DROP INDEX idx_open ON tsla_daily_data;
DROP INDEX idx_volume ON tsla_daily_data;