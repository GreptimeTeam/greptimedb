# Proxy Configuration for DevContainer

This document explains how to configure proxy settings for the GreptimeDB devcontainer.

## Environment Variables

Set these environment variables on your host machine before starting the devcontainer:

```bash
export HTTP_PROXY="http://your-proxy:port"
export HTTPS_PROXY="http://your-proxy:port"
export NO_PROXY="localhost,127.0.0.1,.local,your-company-domain.com"
```

## Usage Examples

### 1. Basic Proxy Setup
```bash
# Set proxy environment variables
export HTTP_PROXY="http://proxy.example.com:8080"
export HTTPS_PROXY="http://proxy.example.com:8080"
export NO_PROXY="localhost,127.0.0.1,.local,internal.company.com"

# Start VS Code with devcontainer
code .
```

### 2. Corporate Proxy with Authentication
```bash
# With authentication
export HTTP_PROXY="http://username:password@proxy.company.com:8080"
export HTTPS_PROXY="http://username:password@proxy.company.com:8080"
export NO_PROXY="localhost,127.0.0.1,.local,company.com,10.0.0.0/8"

code .
```

### 3. SOCKS Proxy
```bash
# SOCKS proxy
export HTTP_PROXY="socks5://127.0.0.1:1080"
export HTTPS_PROXY="socks5://127.0.0.1:1080"
export NO_PROXY="localhost,127.0.0.1,.local"

code .
```

## Docker Build with Proxy

If you need to build the container manually with proxy:

```bash
docker build \
   --network=host \
  --build-arg HTTP_PROXY="http://your-proxy:port" \
  --build-arg HTTPS_PROXY="http://your-proxy:port" \
  --build-arg NO_PROXY="localhost,127.0.0.1,.local" \
  -f .devcontainer/Dockerfile \
  -t greptimedb-devcontainer .
```

## Docker Compose with Proxy

Using docker-compose with proxy:

```bash
# Set environment variables
export HTTP_PROXY="http://your-proxy:port"
export HTTPS_PROXY="http://your-proxy:port"

# Start with docker-compose
docker-compose -f .devcontainer/docker-compose.yml up -d
```

## Troubleshooting

### 1. SSL Certificate Issues
If you encounter SSL certificate issues behind a corporate proxy:

```bash
# Add your company's CA certificate to the container
# Mount the certificate into the container
export DOCKER_BUILDKIT=1
docker build \
  --secret id=ca-cert,src=/path/to/company-ca.crt \
  --build-arg HTTP_PROXY="http://your-proxy:port" \
  --build-arg HTTPS_PROXY="http://your-proxy:port" \
  -f .devcontainer/Dockerfile \
  -t greptimedb-devcontainer .
```

### 2. APT Proxy Issues
If APT (package manager) is not working with proxy:

```bash
# The Dockerfile automatically configures APT proxy
# If issues persist, you can manually configure:
sudo nano /etc/apt/apt.conf.d/95proxies
# Add:
# Acquire::http::Proxy "http://your-proxy:port";
# Acquire::https::Proxy "http://your-proxy:port";
```

### 3. Git Proxy Configuration
For Git operations behind proxy:

```bash
# Inside the container
git config --global http.proxy "http://your-proxy:port"
git config --global https.proxy "http://your-proxy:port"
```

### 4. Cargo (Rust) Proxy Configuration
For Rust package downloads:

```bash
# Inside the container
export CARGO_HTTP_PROXY="http://your-proxy:port"
export CARGO_HTTPS_PROXY="http://your-proxy:port"
```

## Testing Proxy Configuration

Test if proxy is working:

```bash
# Inside the container
curl -I http://httpbin.org/ip
curl -I https://httpbin.org/ip

# Test with proxy variables
http_proxy="http://your-proxy:port" curl -I http://httpbin.org/ip
https_proxy="http://your-proxy:port" curl -I https://httpbin.org/ip
```

## Common Proxy Issues

1. **Connection timeouts**: Increase timeout values or check proxy server
2. **SSL errors**: Update CA certificates or configure SSL verification
3. **Authentication failures**: Verify proxy credentials
4. **DNS resolution**: Ensure proxy can resolve external hostnames

## No Proxy Configuration

If you don't need proxy, simply don't set the environment variables. The devcontainer will work normally without proxy settings.