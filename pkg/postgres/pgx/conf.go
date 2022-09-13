package pgx2q

// Config contains queue table name and max queue size.
type Config struct {
	maxQueue int64
	name     validTableName
}

var DefaultMaxQueue int64 = 15

// ConfigNew creates config with DefaultMaxQueue.
func ConfigNew(tableName string) (Config, error) {
	maxQueue := DefaultMaxQueue
	name, e := validTableNameBuilderPostgres(tableName)
	if nil != e {
		return Config{}, e
	}
	cfg := Config{
		maxQueue,
		name,
	}
	return cfg, nil
}

func (c Config) WithMaxQueue(max int64) Config {
	c.maxQueue = max
	return c
}

func (c Config) MaxQueue() int64           { return c.maxQueue }
func (c Config) ValidName() validTableName { return c.name }
