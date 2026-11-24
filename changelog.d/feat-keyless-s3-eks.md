### Added

- `sqe-server` logs a startup warning when `[storage]` holds no static S3 key and `allow_all_object_stores` or `object_store_admin_roles` is set: those reads run on the pod role or instance profile, past the prefix allowlist

    The book's Kubernetes page has a recipe for keyless S3 reads on EKS (EKS Pod Identity or IRSA), with the IAM, vended-credential and instance-metadata settings that keep the pod role narrow. The server build now fails to compile if it loses the AWS SDK credential chain.
