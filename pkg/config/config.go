package config

import (
	"log"
	"os"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Service   ServiceConfig   `mapstructure:"service"`
	Deps      DepsConfig      `mapstructure:"deps"`
	Minio     MinioConfig     `mapstructure:"minio"`
	Cassandra CassandraConfig `mapstructure:"cassandra"`
	Postgres  PostgresConfig  `mapstructure:"postgres"`
	Kafka     KafkaConfig     `mapstructure:"kafka"`
	Metrics   MetricsConfig   `mapstructure:"metrics"`
	Dev       DevConfig       `mapstructure:"dev"`
	URLs      URLConfig       `mapstructure:"urls"`
}

type ServiceConfig struct {
	Name       string `mapstructure:"name"`
	Env        string `mapstructure:"env"`
	GRPCListen string `mapstructure:"grpc_listen"`
	HTTPListen string `mapstructure:"http_listen"`
	LogLevel   string `mapstructure:"log_level"`
}

type DepsConfig struct {
	FileServiceAddr     string `mapstructure:"file_service_addr"`
	MetadataServiceAddr string `mapstructure:"metadata_service_addr"`
}

type MinioConfig struct {
	Endpoint      string `mapstructure:"endpoint"`
	UseSSL        bool   `mapstructure:"use_ssl"`
	BucketDefault string `mapstructure:"bucket_default"`
	AccessKey     string `mapstructure:"access_key"`
	SecretKey     string `mapstructure:"secret_key"`
}

type CassandraConfig struct {
	Enabled     bool     `mapstructure:"enabled"`
	Hosts       []string `mapstructure:"hosts"`
	Port        int      `mapstructure:"port"`
	Keyspace    string   `mapstructure:"keyspace"`
	Consistency string   `mapstructure:"consistency"`
}

type PostgresConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	DB       string `mapstructure:"db"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	SSLMode  string `mapstructure:"sslmode"`
}

type KafkaConfig struct {
	Brokers   		[]string `mapstructure:"brokers"`
	ClientID  		string   `mapstructure:"client_id"`
	Topic     		string   `mapstructure:"topic_events"`
	ProducersAmount int		 `mapstructure:"producers_amount"`
}

type MetricsConfig struct {
	Listen    string `mapstructure:"listen"`
	Namespace string `mapstructure:"namespace"`
}

type DevConfig struct {
	EnableHotReload bool `mapstructure:"enable_hot_reload"`
}

type URLConfig struct {
	Handlers map[string]string `mapstructure:"handlers"`
}

func LoadConfig(path string) *Config {
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(path)

	v.SetEnvPrefix("FS")
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "__"))

	if err := v.ReadInConfig(); err != nil {
		log.Printf("config not found (%s): %v — will use env/defaults", path, err)
	}

	v.SetDefault("service.env", "dev")
	v.SetDefault("metrics.listen", "0.0.0.0:2112")
	v.SetDefault("service.grpc_listen", "0.0.0.0:50051")
	v.SetDefault("service.http_listen", "0.0.0.0:8080")
	v.SetDefault("urls.handlers.health", "/health")

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		log.Fatalf("error unmarshalling config: %v", err)
	}
	
	if cfg.Minio.AccessKey == "" {
		cfg.Minio.AccessKey = os.Getenv("MINIO_ACCESS_KEY")
	}
	if cfg.Minio.SecretKey == "" {
		cfg.Minio.SecretKey = os.Getenv("MINIO_SECRET_KEY")
	}

	return &cfg
}