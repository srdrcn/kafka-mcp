# kafka-mcp — build, test and image
IMAGE ?= kafka-mcp:latest
CONTAINER_TOOL ?= $(shell command -v podman 2>/dev/null || command -v docker 2>/dev/null)
CONFIG ?= kafka-mcp.yaml

.PHONY: build test image run run-stdio snapshot clean

build: ## Runnable jar: target/kafka-mcp.jar
	./mvnw -B package -DskipTests

test: ## All tests (Testcontainers needs Docker/Podman)
	./mvnw -B verify

image: ## Container image (multi-stage, UBI9 OpenJDK 21)
	$(CONTAINER_TOOL) build -t $(IMAGE) .

run: build ## Run locally over HTTP (port from the config) or stdio
	java -jar target/kafka-mcp.jar --config $(CONFIG)

run-stdio: build
	java -jar target/kafka-mcp.jar --config $(CONFIG) --transport stdio

snapshot: ## Update the tool schema snapshots
	./mvnw -B test -Dtest=ToolSchemaSnapshotTest -Dsnapshot.update=true

clean:
	./mvnw -B -q clean
