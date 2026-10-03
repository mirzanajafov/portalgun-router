FROM envoyproxy/envoy:v1.39.2
COPY envoy.yaml /etc/envoy/envoy.yaml
CMD ["envoy", "-c", "/etc/envoy/envoy.yaml", "--concurrency", "1", "--log-level", "warn"]
