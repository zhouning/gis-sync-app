#!/usr/bin/env bash
# 下载 Flink 运行所需的 connector / format / Confluent SR client jar 到 docker/flink-libs/
# 这些 jar 在 pom.xml 中是 provided scope，docker-compose 启动时会拷贝到 Flink lib/
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="$DIR/flink-libs"
mkdir -p "$LIB_DIR"

# ---- 1. Flink connectors / formats（直接 URL 拉，固定版本）----
declare -a JARS=(
  # Flink CDC 3.5.0（最后兼容 Flink 1.19 的版本，3.6+ 砍了 1.19）
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.5.0/flink-sql-connector-postgres-cdc-3.5.0.jar"
  # JDBC connector 3.3.0-1.19（1.19 系列最新）
  "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.19/flink-connector-jdbc-3.3.0-1.19.jar"
  # Kafka connector（fat jar，含 SQL+DataStream 两套 API）
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar"
  # Avro Confluent Schema Registry format（DataStream + SQL 两用版本，不要用 sql- 前缀的）
  "https://repo1.maven.org/maven2/org/apache/flink/flink-avro-confluent-registry/1.19.3/flink-avro-confluent-registry-1.19.3.jar"
  "https://repo1.maven.org/maven2/org/apache/flink/flink-avro/1.19.3/flink-avro-1.19.3.jar"
  "https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.4/avro-1.11.4.jar"
  # PostgreSQL JDBC driver
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
  # Jackson 2.16（被 SR client 需要，必须版本对齐）
  "https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.16.0/jackson-core-2.16.0.jar"
)

for url in "${JARS[@]}"; do
  fname="$(basename "$url")"
  if [[ -f "$LIB_DIR/$fname" ]]; then
    echo "[skip] $fname"
  else
    echo "[get ] $fname"
    curl -fsSL --retry 3 -o "$LIB_DIR/$fname" "$url"
  fi
done

# ---- 2. Confluent SR client + 传递依赖（用 Maven 解析，因为依赖链长）----
# 通过临时 pom + dependency:copy-dependencies 把整链拉到 LIB_DIR
SR_MARKER="$LIB_DIR/.sr-client-installed"
if [[ -f "$SR_MARKER" ]]; then
  echo "[skip] Confluent SR client (marker exists)"
else
  echo "[get ] Confluent SR client + transitive deps"
  TMP_POM=$(mktemp -d)
  cat > "$TMP_POM/pom.xml" <<'EOF'
<?xml version="1.0"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>tmp</groupId><artifactId>sr</artifactId><version>1</version>
  <repositories>
    <repository><id>confluent</id><url>https://packages.confluent.io/maven/</url></repository>
  </repositories>
  <dependencies>
    <dependency>
      <groupId>io.confluent</groupId>
      <artifactId>kafka-schema-registry-client</artifactId>
      <version>7.7.1</version>
    </dependency>
    <dependency>
      <groupId>io.confluent</groupId>
      <artifactId>kafka-avro-serializer</artifactId>
      <version>7.7.1</version>
    </dependency>
  </dependencies>
</project>
EOF
  docker run --rm \
    -v "$TMP_POM/pom.xml":/workspace/pom.xml \
    -v "$TMP_POM/dep":/workspace/target/dependency \
    -v gis-maven-cache:/root/.m2 \
    -w /workspace \
    maven:3.9-eclipse-temurin-17 \
    mvn -B dependency:copy-dependencies -DincludeScope=runtime -Dsilent=true >/dev/null

  # 拷贝时跳过与 Flink lib 已有 jar 冲突的版本
  for f in "$TMP_POM/dep"/*.jar; do
    name="$(basename "$f")"
    case "$name" in
      avro-1.11.3.jar)        echo "  [skip-conflict] $name" ; continue ;;
      slf4j-api-*.jar)        echo "  [skip-conflict] $name" ; continue ;;
      jackson-core-2.14.*)    echo "  [skip-conflict] $name (using 2.16.0 above)" ; continue ;;
    esac
    cp "$f" "$LIB_DIR/$name"
  done
  rm -rf "$TMP_POM"
  touch "$SR_MARKER"
fi

echo ""
echo "Done. Jars in $LIB_DIR ($(ls "$LIB_DIR" | grep -c '.jar$') total):"
ls -lh "$LIB_DIR" | tail -n +2 | awk '{printf "  %-50s %s\n", $9, $5}'
