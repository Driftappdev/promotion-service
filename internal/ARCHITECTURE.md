# Architecture Baseline
This service follows the shared microservice layout baseline:
- cmd: entrypoint
- config: configuration loading/static config
- internal/adapter: inbound/outbound adapters
- internal/dto: transport/contracts
- internal/interface: ports/interfaces
- internal/mapper: mapping between layers
- internal/model: domain/data models
- internal/pkg: shared wrappers/helpers for this service
- internal/route: route registration
- internal/servicecore: shared service infra hooks
- internal/service_logic: handlers/services/workers
Legacy folders are intentionally preserved for backward compatibility.
