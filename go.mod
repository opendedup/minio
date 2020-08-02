module github.com/minio/minio

go 1.13

require (
	cloud.google.com/go v0.56.0
	cloud.google.com/go/storage v1.6.0
	git.apache.org/thrift.git v0.13.0
	github.com/Azure/azure-pipeline-go v0.2.1
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Azure/go-autorest/autorest/adal v0.9.0 // indirect
	github.com/Shopify/sarama v1.24.1
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/alecthomas/participle v0.2.1
	github.com/aws/aws-sdk-go v1.20.21
	github.com/bcicen/jstream v0.0.0-20190220045926-16c1f8af81c2
	github.com/beevik/ntp v0.2.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/cheggaaa/pb v1.0.28
	github.com/cockroachdb/datadriven v0.0.0-20190809214429-80d97fb3cbaa // indirect
	github.com/colinmarc/hdfs/v2 v2.1.1
	github.com/coredns/coredns v1.4.0
	github.com/coreos/etcd v3.3.22+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/creack/pty v1.1.7 // indirect
	github.com/dchest/siphash v1.2.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/divan/depscheck v0.0.0-20181211143037-d54c5bee1b11 // indirect
	github.com/djherbis/atime v1.0.0
	github.com/dustin/go-humanize v1.0.0
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/fatih/color v1.7.0
	github.com/fatih/structs v1.1.0
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/golang/protobuf v1.4.2
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/uuid v1.1.1
	github.com/gorilla/handlers v1.4.2
	github.com/gorilla/mux v1.7.5-0.20200711200521-98cb6bf42e08
	github.com/gorilla/rpc v1.2.0
	github.com/gorilla/websocket v0.0.0-20170926233335-4201258b820c // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.1-0.20190118093823-f849b5445de4 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.9.5 // indirect
	github.com/hashicorp/vault/api v1.0.4
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/json-iterator/go v1.1.10
	github.com/klauspost/compress v1.10.3
	github.com/klauspost/cpuid v1.3.1
	github.com/klauspost/pgzip v1.2.1
	github.com/klauspost/readahead v1.3.1
	github.com/klauspost/reedsolomon v1.9.9
	github.com/lib/pq v1.7.0
	github.com/mattn/go-colorable v0.1.4
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.8
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/miekg/dns v1.1.8
	github.com/minio/cli v1.22.0
	github.com/minio/highwayhash v1.0.0
	github.com/minio/minio-go/v7 v7.0.1
	github.com/minio/selfupdate v0.3.0
	github.com/minio/sha256-simd v0.1.1
	github.com/minio/simdjson-go v0.1.5-0.20200303142138-b17fe061ea37
	github.com/minio/sio v0.2.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mmcloughlin/avo v0.0.0-20200523190732-4439b6b2c061 // indirect
	github.com/montanaflynn/stats v0.5.0
	github.com/nats-io/nats-server/v2 v2.1.7
	github.com/nats-io/nats-streaming-server v0.18.0 // indirect
	github.com/nats-io/nats.go v1.10.0
	github.com/nats-io/stan.go v0.7.0
	github.com/ncw/directio v1.0.5
	github.com/nsqio/go-nsq v1.0.7
	github.com/olekukonko/tablewriter v0.0.0-20170122224234-a0225b3f23b5 // indirect
	github.com/opendedup/sdfs-client-go v0.1.1
	github.com/ozonru/etcd v3.3.20+incompatible
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pierrec/lz4 v2.4.0+incompatible
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.0.0
	github.com/ramya-rao-a/go-outline v0.0.0-20200117021646-2a048b4510eb // indirect
	github.com/rjeczalik/notify v0.9.2
	github.com/rs/cors v1.7.0
	github.com/secure-io/sio-go v0.3.0
	github.com/shirou/gopsutil v2.20.3-0.20200314133625-53cec6b37e6a+incompatible
	github.com/sirupsen/logrus v1.6.0
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cobra v0.0.3 // indirect
	github.com/spf13/pflag v1.0.1 // indirect
	github.com/streadway/amqp v0.0.0-20190404075320-75d898a42a94
	github.com/tidwall/gjson v1.3.5
	github.com/tidwall/sjson v1.0.4
	github.com/tinylib/msgp v1.1.2
	github.com/tmc/grpc-websocket-proxy v0.0.0-20170815181823-89b8d40f7ca8 // indirect
	github.com/urfave/cli v1.20.0 // indirect
	github.com/valyala/tcplisten v0.0.0-20161114210144-ceec8f93295a
	github.com/willf/bitset v1.1.10 // indirect
	github.com/willf/bloom v2.0.3+incompatible
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.uber.org/zap v1.15.0 // indirect
	golang.org/x/crypto v0.0.0-20200709230013-948cd5f35899
	golang.org/x/net v0.0.0-20200707034311-ab3426394381
	golang.org/x/sys v0.0.0-20200728102440-3e129f6d46b1
	golang.org/x/text v0.3.3 // indirect
	google.golang.org/api v0.29.0
	google.golang.org/grpc v1.30.0
	gopkg.in/cheggaaa/pb.v1 v1.0.25 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0
	gopkg.in/ldap.v3 v3.0.3
	gopkg.in/olivere/elastic.v5 v5.0.80
	gopkg.in/yaml.v2 v2.2.8
	sigs.k8s.io/yaml v1.1.0 // indirect
)
