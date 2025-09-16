# TLS Security Guide

This document explains how to configure and use TLS (Transport Layer Security) in Go-RocketMQ for secure network communication.

## Overview

Go-RocketMQ supports TLS encryption for all network communications between components (NameServer, Broker, Producer, Consumer). This ensures that data transmitted over the network is encrypted and protected from eavesdropping and tampering.

## Configuration

### Enabling TLS

To enable TLS, you need to configure the TLS settings in the `config.yaml` file for each component:

```yaml
# NameServer 配置
nameserver:
  listen_port: 9876
  # TLS配置
  tls:
    enable: true
    cert_file: "/path/to/server.crt"
    key_file: "/path/to/server.key"
    ca_file: "/path/to/ca.crt"
    server_name: "nameserver.example.com"
    skip_verify: false

# Broker 配置  
broker:
  listen_port: 10911
  # TLS配置
  tls:
    enable: true
    cert_file: "/path/to/server.crt"
    key_file: "/path/to/server.key"
    ca_file: "/path/to/ca.crt"
    server_name: "broker.example.com"
    skip_verify: false

# Producer 配置
producer:
  # TLS配置
  tls:
    enable: true
    cert_file: "/path/to/client.crt"
    key_file: "/path/to/client.key"
    ca_file: "/path/to/ca.crt"
    server_name: "broker.example.com"
    skip_verify: false

# Consumer 配置
consumer:
  # TLS配置
  tls:
    enable: true
    cert_file: "/path/to/client.crt"
    key_file: "/path/to/client.key"
    ca_file: "/path/to/ca.crt"
    server_name: "broker.example.com"
    skip_verify: false
```

### TLS Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `enable` | Whether to enable TLS | `false` |
| `cert_file` | Path to the certificate file | `""` |
| `key_file` | Path to the private key file | `""` |
| `ca_file` | Path to the CA certificate file | `""` |
| `server_name` | Server name for certificate verification | `""` |
| `skip_verify` | Whether to skip certificate verification | `false` |

## Certificate Generation

For testing purposes, you can generate self-signed certificates using OpenSSL:

```bash
# Generate CA certificate
openssl genrsa -out ca.key 2048
openssl req -new -x509 -key ca.key -out ca.crt -days 365

# Generate server certificate
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365

# Generate client certificate
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365
```

## Connection Pool with TLS

The connection pool automatically handles TLS connections when TLS is enabled in the configuration:

```go
// Create connection pool with TLS configuration
poolConfig := &connection.ConnectionPoolConfig{
    MaxConnections: 100,
    ConnectTimeout: 3 * time.Second,
    RequestTimeout: 30 * time.Second,
    TLSConfig: &connection.TLSConfig{
        EnableTLS:  true,
        CertFile:   "/path/to/client.crt",
        KeyFile:    "/path/to/client.key",
        CAFile:     "/path/to/ca.crt",
        ServerName: "broker.example.com",
        SkipVerify: false,
    },
}

pool := connection.NewConnectionPool(poolConfig)
```

## Security Best Practices

1. **Use Strong Certificates**: Always use certificates from a trusted Certificate Authority (CA) in production environments.

2. **Enable Certificate Verification**: Never set `skip_verify` to `true` in production as it makes connections vulnerable to man-in-the-middle attacks.

3. **Regular Certificate Rotation**: Rotate certificates regularly to minimize the impact of potential certificate compromises.

4. **Protect Private Keys**: Store private keys securely and restrict access to them.

5. **Use Strong Cipher Suites**: Configure your TLS settings to use strong cipher suites and protocols.

6. **Monitor Certificate Expiry**: Set up monitoring to alert when certificates are approaching expiry.

## Performance Considerations

TLS encryption adds some overhead to network communications. Consider the following:

1. **Connection Reuse**: Use connection pooling to minimize the overhead of TLS handshakes.

2. **Session Resumption**: Enable TLS session resumption to reduce handshake overhead for subsequent connections.

3. **Hardware Acceleration**: Use hardware acceleration for cryptographic operations if available.

## Troubleshooting

### Common Issues

1. **Certificate Verification Failures**: Ensure that the CA certificate is correctly configured and that the server certificate is valid.

2. **Hostname Mismatch**: Make sure the `server_name` in the configuration matches the Common Name (CN) or Subject Alternative Name (SAN) in the server certificate.

3. **Connection Timeouts**: TLS handshakes can take longer than regular TCP connections. Adjust timeout settings accordingly.

### Debugging

Enable debug logging to get more information about TLS connection issues:

```yaml
logging:
  level: "debug"
```

## Example Usage

Here's a complete example of configuring TLS for a producer:

```go
// Load configuration
config := &ProducerConfig{
    NamesrvAddr: "127.0.0.1:9876",
    TLSConfig: &TLSConfig{
        EnableTLS:  true,
        CertFile:   "/path/to/client.crt",
        KeyFile:    "/path/to/client.key",
        CAFile:     "/path/to/ca.crt",
        ServerName: "nameserver.example.com",
        SkipVerify: false,
    },
}

// Create producer with TLS
producer := NewProducer(config)
err := producer.Start()
if err != nil {
    log.Fatal("Failed to start producer:", err)
}
```

This implementation provides enterprise-grade security for your Go-RocketMQ deployments while maintaining high performance through connection pooling and efficient TLS handling.