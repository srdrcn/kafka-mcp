# kafka-mcp — derleme, test ve imaj
IMAGE ?= kafka-mcp:latest
CONTAINER_TOOL ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
CONFIG ?= kafka-mcp.yaml

.PHONY: build test image run run-stdio snapshot clean

build: ## Çalıştırılabilir jar: target/kafka-mcp.jar
	./mvnw -B package -DskipTests

test: ## Tüm testler (Testcontainers için Docker/Podman gerekir)
	./mvnw -B verify

image: ## Container imajı (multi-stage, UBI9 OpenJDK 21)
	$(CONTAINER_TOOL) build -t $(IMAGE) .

run: build ## Yerelde HTTP (config'teki port) ya da stdio
	java -jar target/kafka-mcp.jar --config $(CONFIG)

run-stdio: build
	java -jar target/kafka-mcp.jar --config $(CONFIG) --transport stdio

snapshot: ## Araç şeması snapshot'larını güncelle
	./mvnw -B test -Dtest=ToolSchemaSnapshotTest -Dsnapshot.update=true

clean:
	./mvnw -B -q clean
