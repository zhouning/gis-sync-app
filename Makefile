SHELL := /bin/bash
COMPOSE := docker compose -f docker/docker-compose.yml
JAR := flink-jobs/target/gis-sync-app-1.0-SNAPSHOT.jar
MAVEN_IMAGE := maven:3.9-eclipse-temurin-17

.PHONY: build build-flink build-backend libs up down restart logs ps psql-src psql-dst kafka-topics kafka-consume sr-subjects submit-datagen submit-source submit-sink submit-cdc submit-geofence cancel-all clean rebuild ha-leader job-list savepoint ha-kill-leader flink-metrics prom-targets prom-alerts prom-reload grafana backend-logs api fe-install fe-dev fe-build smoke test it

# 用 Docker 跑 mvn，无需本机安装 JDK/Maven
build:
	docker run --rm \
	  -v $(PWD):/workspace \
	  -v gis-maven-cache:/root/.m2 \
	  -w /workspace \
	  $(MAVEN_IMAGE) mvn -B clean package -DskipTests

build-flink:
	docker run --rm \
	  -v $(PWD):/workspace \
	  -v gis-maven-cache:/root/.m2 \
	  -w /workspace \
	  $(MAVEN_IMAGE) mvn -B -pl flink-jobs -am clean package -DskipTests

build-backend:
	docker run --rm \
	  -v $(PWD):/workspace \
	  -v gis-maven-cache:/root/.m2 \
	  -w /workspace \
	  $(MAVEN_IMAGE) mvn -B -pl backend -am clean package -DskipTests

# 下载 Flink connector jars（CDC + JDBC + PG driver）
libs:
	bash docker/fetch-libs.sh

# 启动 PostGIS + Flink 集群
up: libs
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

# mvn clean 重建 target 后 bind mount 在 macOS 上需要 restart 才能识别新 jar
restart:
	$(COMPOSE) restart jobmanager taskmanager

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

psql-src:
	docker exec -it gis-postgis-src psql -U postgres -d geodb_src

psql-dst:
	docker exec -it gis-postgis-dst psql -U postgres -d geodb_dst

kafka-topics:
	docker exec gis-kafka kafka-topics --bootstrap-server kafka:9094 --list

kafka-consume:
	@if [ -z "$(TOPIC)" ]; then echo "usage: make kafka-consume TOPIC=spatial-data-cdc"; exit 1; fi
	docker exec -it gis-kafka kafka-console-consumer --bootstrap-server kafka:9094 --topic $(TOPIC) --from-beginning

sr-subjects:
	@curl -s http://localhost:8082/subjects | python3 -m json.tool

submit-source:
	docker exec gis-flink-jobmanager flink run -d -c com.example.gis.source.SourceCdcToKafkaJob /opt/flink/usrlib/gis-sync-app-1.0-SNAPSHOT.jar

submit-sink:
	docker exec gis-flink-jobmanager flink run -d -c com.example.gis.sink.KafkaToSinkJob /opt/flink/usrlib/gis-sync-app-1.0-SNAPSHOT.jar

# 兼容旧别名
submit-cdc: submit-source

# === HA / savepoint 操作 ===

# 列出 ZK 里注册的 leader（验证 HA）
ha-leader:
	@docker exec gis-zookeeper bash -c "echo 'get /flink/gis-sync/leader/rest_server_lock' | zkCli.sh -server localhost:2181 2>/dev/null | tail -5"

# 列出所有 RUNNING 作业的 jid
job-list:
	@curl -s http://localhost:8081/jobs | python3 -c "import json,sys; \
		[print(f\"{j['id']}  {j.get('status','?')}\") for j in json.load(sys.stdin)['jobs']]"

# 触发 savepoint（一次性快照，作业继续跑）。用法：make savepoint JOB=<jid>
savepoint:
	@if [ -z "$(JOB)" ]; then echo "usage: make savepoint JOB=<jid>"; exit 1; fi
	@curl -s -X POST -H 'Content-Type: application/json' \
		-d '{"target-directory":"file:///flink-savepoints","cancel-job":false}' \
		http://localhost:8081/jobs/$(JOB)/savepoints | python3 -m json.tool

# 把 leader JM 杀掉，观察 #2 接管
ha-kill-leader:
	@LEADER=$$(docker ps --format '{{.Names}}' | grep -E 'gis-flink-jobmanager$$|gis-flink-jobmanager2$$' \
		| while read c; do code=$$(docker exec $$c sh -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/overview' 2>/dev/null); \
		[ "$$code" = "200" ] && echo $$c; done | head -1); \
		echo "Leader appears to be: $$LEADER"; \
		[ -n "$$LEADER" ] && docker kill $$LEADER

# Prometheus reporter 端口探活
flink-metrics:
	@echo "JobManager:"  && curl -fsS http://localhost:9249/metrics 2>&1 | grep -c '^flink_' || true
	@echo "JobManager2:" && curl -fsS http://localhost:9250/metrics 2>&1 | grep -c '^flink_' || true
	@echo "TaskManager:" && curl -fsS http://localhost:9251/metrics 2>&1 | grep -c '^flink_' || true

# === 监控 ===
prom-targets:
	@curl -s http://localhost:9090/api/v1/targets | python3 -c "import json,sys; \
		ts = json.load(sys.stdin)['data']['activeTargets']; \
		[print(f\"  {t['labels'].get('job','?'):20s}  {t['health']:10s}  {t['scrapeUrl']}\") for t in ts]"

prom-alerts:
	@curl -s http://localhost:9090/api/v1/alerts | python3 -m json.tool

prom-reload:
	@curl -fsS -X POST http://localhost:9090/-/reload && echo "  reloaded"

grafana:
	@echo "Grafana: http://localhost:3000  (anonymous viewer enabled)"
	@echo "Admin:   admin / admin"

# === 后端 ===
backend-logs:
	$(COMPOSE) logs -f --tail=100 backend

# 快速 smoke test 各端点
api:
	@echo "--- /api/sync/status ---"
	@curl -fsS http://localhost:8090/api/sync/status | python3 -m json.tool | head -30
	@echo ""
	@echo "--- /api/slot/health ---"
	@curl -fsS http://localhost:8090/api/slot/health | python3 -m json.tool
	@echo ""
	@echo "--- /api/sync/metrics (默认最近 30 分钟) ---"
	@curl -fsS http://localhost:8090/api/sync/metrics | python3 -m json.tool | head -20
	@echo ""
	@echo "--- /api/dlq?limit=5 ---"
	@curl -fsS 'http://localhost:8090/api/dlq?limit=5' | python3 -m json.tool | head -30
	@echo ""
	@echo "--- /api/sync/live-points?limit=3 ---"
	@curl -fsS 'http://localhost:8090/api/sync/live-points?limit=3' | python3 -m json.tool

# === 前端 ===
fe-install:
	cd frontend && npm install --cache /tmp/npm-cache-gis

fe-dev:
	cd frontend && npm run dev

fe-build:
	cd frontend && npm run build

# === 测试 / CI ===
test:
	docker run --rm \
	  -v $(PWD):/workspace \
	  -v gis-maven-cache:/root/.m2 \
	  -w /workspace \
	  $(MAVEN_IMAGE) mvn -B -pl flink-jobs -am test

# 集成测试需要 Docker 在主机上可用，所以不放在 maven docker 里跑
it:
	mvn -B -pl flink-jobs -am verify

smoke:
	bash scripts/smoke.sh

smoke-keep:
	bash scripts/smoke.sh --keep

# 持续生成模拟同步流量，让看板看到实时变化（Ctrl-C 停）
# INTERVAL（秒，默认 2）/ DURATION（秒，默认 0=无限）可覆盖
demo-traffic:
	bash scripts/demo-traffic.sh

# 快速密集流量（看板更"活"）
demo-traffic-fast:
	INTERVAL=1 bash scripts/demo-traffic.sh

# 取消所有 RUNNING 作业
cancel-all:
	@for jid in $$(curl -s http://localhost:8081/jobs | python3 -c "import json,sys;print(' '.join(j['id'] for j in json.load(sys.stdin)['jobs'] if j['status']=='RUNNING'))"); do \
	  echo "Cancelling $$jid"; \
	  curl -s -X PATCH "http://localhost:8081/jobs/$$jid?mode=cancel"; \
	done

# 改完代码后的标准流程
rebuild: build restart
	@echo "Rebuilt and restarted Flink. Resubmit jobs as needed."

clean:
	rm -rf target docker/flink-libs
