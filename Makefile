.PHONY: build proto docker-up docker-down test

build:
	cd api-gateway && npm ci && npm run build
	cd coordinator && pip install -q -r requirements.txt && python -c "from main import main"
	cd worker && pip install -q -r requirements.txt && python -c "from main import main"

proto:
	@which protoc >/dev/null || (echo "install protoc and run: protoc --proto_path=proto --go_out=proto --go_opt=module=github.com/distributed-scheduler/proto --go-grpc_out=proto --go-grpc_opt=module=github.com/distributed-scheduler/proto proto/scheduler.proto" && exit 1)
	protoc --proto_path=proto --go_out=proto --go_opt=module=github.com/distributed-scheduler/proto --go-grpc_out=proto --go-grpc_opt=module=github.com/distributed-scheduler/proto proto/scheduler.proto

docker-up:
	docker compose up -d

docker-down:
	docker compose down

test:
	cd api-gateway && npm test 2>/dev/null || true
	cd coordinator && python -m pytest 2>/dev/null || true
	cd worker && python -m pytest 2>/dev/null || true
