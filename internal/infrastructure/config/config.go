package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	Database  DatabaseConfig  `mapstructure:"database"`
	OSS       OSSConfig       `mapstructure:"oss"`
	Hive      HiveConfig      `mapstructure:"hive"`
	StarRocks StarRocksConfig `mapstructure:"starrocks"`
	Archive   ArchiveConfig   `mapstructure:"archive"`
	Audit     AuditConfig     `mapstructure:"audit"`
	Log       LogConfig       `mapstructure:"log"`
}

type ServerConfig struct {
	Port         int    `mapstructure:"port"`
	Mode         string `mapstructure:"mode"`
	ReadTimeout  int    `mapstructure:"read_timeout"`
	WriteTimeout int    `mapstructure:"write_timeout"`
}

type DatabaseConfig struct {
	Driver      string `mapstructure:"driver"`
	Host        string `mapstructure:"host"`
	Port        int    `mapstructure:"port"`
	User        string `mapstructure:"user"`
	Password    string `mapstructure:"password"`
	DBName      string `mapstructure:"dbname"`
	MaxOpenConn int    `mapstructure:"max_open_conn"`
	MaxIdleConn int    `mapstructure:"max_idle_conn"`
	MaxLifetime int    `mapstructure:"max_lifetime"`
}

type OSSConfig struct {
	Endpoint  string `mapstructure:"endpoint"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	Bucket    string `mapstructure:"bucket"`
	PathPrefix string `mapstructure:"path_prefix"`
	Region    string `mapstructure:"region"`
	UseSSL    bool   `mapstructure:"use_ssl"`
}

type HiveConfig struct {
	MetastoreURI string `mapstructure:"metastore_uri"`
	Database     string `mapstructure:"database"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
}

type StarRocksConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	LoadURL  string `mapstructure:"load_url"`
}

type ArchiveConfig struct {
	ColdYears       int           `mapstructure:"cold_years"`
	ShardCount      int           `mapstructure:"shard_count"`
	Concurrency     int           `mapstructure:"concurrency"`
	BatchSize       int           `mapstructure:"batch_size"`
	MaxRetryCount   int           `mapstructure:"max_retry_count"`
	MemoryLimitMB   int64         `mapstructure:"memory_limit_mb"`
	StreamBufferSize int          `mapstructure:"stream_buffer_size"`
	MaskingSalt     string        `mapstructure:"masking_salt"`
	TableName       string        `mapstructure:"table_name"`
	TargetTable     string        `mapstructure:"target_table"`
	CronExpr        string        `mapstructure:"cron_expr"`
}

type AuditConfig struct {
	Enabled               bool    `mapstructure:"enabled"`
	SampleRate            float64 `mapstructure:"sample_rate"`
	BatchSize             int     `mapstructure:"batch_size"`
	Concurrency           int     `mapstructure:"concurrency"`
	QueueCapacity         int     `mapstructure:"queue_capacity"`
	WebhookURL            string  `mapstructure:"webhook_url"`
	WebhookTimeoutSec     int     `mapstructure:"webhook_timeout_sec"`
	MaxRetries            int     `mapstructure:"max_retries"`
	AlertThresholdMiss    float64 `mapstructure:"alert_threshold_miss"`
	AlertThresholdMismatch float64 `mapstructure:"alert_threshold_mismatch"`
}

type LogConfig struct {
	Level    string `mapstructure:"level"`
	Format   string `mapstructure:"format"`
	FilePath string `mapstructure:"file_path"`
	MaxSize  int    `mapstructure:"max_size"`
	MaxBackups int  `mapstructure:"max_backups"`
	MaxAge   int    `mapstructure:"max_age"`
}

func Load(configPath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(configPath)
	v.SetConfigType("yaml")

	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config error: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config error: %w", err)
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.mode", "release")
	v.SetDefault("server.read_timeout", 60)
	v.SetDefault("server.write_timeout", 60)

	v.SetDefault("database.driver", "mysql")
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 3306)
	v.SetDefault("database.max_open_conn", 100)
	v.SetDefault("database.max_idle_conn", 20)
	v.SetDefault("database.max_lifetime", 3600)

	v.SetDefault("oss.path_prefix", "archive")
	v.SetDefault("oss.use_ssl", true)

	v.SetDefault("hive.database", "default")

	v.SetDefault("starrocks.port", 9030)
	v.SetDefault("starrocks.database", "unicorn")

	v.SetDefault("archive.cold_years", 3)
	v.SetDefault("archive.shard_count", 10)
	v.SetDefault("archive.concurrency", 5)
	v.SetDefault("archive.batch_size", 1000)
	v.SetDefault("archive.max_retry_count", 3)
	v.SetDefault("archive.memory_limit_mb", 512)
	v.SetDefault("archive.stream_buffer_size", 100)
	v.SetDefault("archive.masking_salt", "archive-salt-default")
	v.SetDefault("archive.table_name", "order_detail")
	v.SetDefault("archive.target_table", "unicorn_pro_history")

	v.SetDefault("audit.enabled", true)
	v.SetDefault("audit.sample_rate", 0.1)
	v.SetDefault("audit.batch_size", 100)
	v.SetDefault("audit.concurrency", 2)
	v.SetDefault("audit.queue_capacity", 10000)
	v.SetDefault("audit.webhook_url", "")
	v.SetDefault("audit.webhook_timeout_sec", 10)
	v.SetDefault("audit.max_retries", 3)
	v.SetDefault("audit.alert_threshold_miss", 0.05)
	v.SetDefault("audit.alert_threshold_mismatch", 0.05)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
}

func (c *ArchiveConfig) ColdDate() time.Time {
	return time.Now().AddDate(-c.ColdYears, 0, 0)
}

func (c *ArchiveConfig) MemoryLimitBytes() int64 {
	return c.MemoryLimitMB * 1024 * 1024
}
