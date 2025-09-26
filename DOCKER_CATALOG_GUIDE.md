# Docker Image & Catalog Setup Guide

## Overview

For Git-based catalogs, the workflow is:

1. **Build & Push Docker Images** → Get SHA256 digests
2. **Update Catalog** → Use digest references (not tags)
3. **Commit Catalog** → Push to Git repository
4. **Configure Catalog Source** → Point Noesis to your Git catalog

## Quick Start

### 1. Choose a Docker Registry

You need to push your images to a public registry. Options:

**Docker Hub (Recommended for public projects):**
```bash
# Login
docker login

# Use prefix: docker.io/yourusername
# Example: docker.io/datapower/noesis-postgres-connector
```

**GitHub Container Registry (GHCR):**
```bash
# Login with personal access token
echo $GITHUB_TOKEN | docker login ghcr.io -u yourusername --password-stdin

# Use prefix: ghcr.io/yourusername
# Example: ghcr.io/data-power-io/noesis-postgres-connector
```

### 2. Build and Push Connector

Use the automated script:

```bash
# For Docker Hub
./scripts/build-and-push-connector.sh postgres docker.io/yourusername

# For GHCR
./scripts/build-and-push-connector.sh postgres ghcr.io/yourusername
```

### 3. Manual Method (if script doesn't work)

```bash
# Build
cd connectors/postgres
docker build -t yourusername/noesis-postgres-connector:1.0.0 .

# Push
docker push yourusername/noesis-postgres-connector:1.0.0

# Get digest (IMPORTANT!)
docker inspect yourusername/noesis-postgres-connector:1.0.0 --format='{{index .RepoDigests 0}}'
# Or pull again to see digest:
docker pull yourusername/noesis-postgres-connector:1.0.0
```

### 4. Update Catalog with Digest

Edit `catalog/index.json` and update the image field:

```json
{
  "connectors": {
    "postgres": {
      "image": "docker.io/yourusername/noesis-postgres-connector@sha256:abcd1234567890...",
      ...
    }
  }
}
```

**⚠️ CRITICAL:** Must use `@sha256:digest` format, NOT `:tag` format!

### 5. Commit and Push to Git

```bash
git add catalog/index.json
git commit -m "Update postgres connector with Docker digest"
git push origin main
```

### 6. Add Catalog Source in Noesis

In your Noesis admin dashboard:

1. Go to **Admin** → **Catalog Management**
2. Click **Add Catalog Source**
3. Choose **Git Repository**
4. Enter URL: `https://github.com/data-power-io/noesis-connectors#main:catalog/index.json`

## Catalog URL Formats

### Git Repository Format
```
https://github.com/user/repo#branch:path/to/catalog.json
```

Examples:
- `https://github.com/data-power-io/noesis-connectors#main:catalog/index.json`
- `https://github.com/data-power-io/noesis-connectors#v1.0.0:catalog/index.json`
- `https://github.com/data-power-io/noesis-connectors#develop:staging/catalog.json`

### Raw GitHub URL (alternative)
```
https://raw.githubusercontent.com/user/repo/branch/path/catalog.json
```

Example:
- `https://raw.githubusercontent.com/data-power-io/noesis-connectors/main/catalog/index.json`

## How the Catalog-Worker Processes Images

1. **Reads Catalog**: Fetches catalog from Git URL
2. **Validates Format**: Ensures catalog follows schema
3. **Extracts Images**: Gets digest-based image references
4. **Pulls Images**: Uses Docker to pull images by digest
5. **Stores Metadata**: Saves connector info in database

## Image Digest Requirements

The catalog format **requires** digest-based image references for security:

✅ **Valid:**
```json
"image": "docker.io/datapower/connector@sha256:abcd1234567890abcdef..."
```

❌ **Invalid:**
```json
"image": "docker.io/datapower/connector:latest"
"image": "docker.io/datapower/connector:v1.0.0"
```

## Registry Security

### Allowed Registry Prefixes

When adding a catalog source, you can restrict which registries are allowed:

```json
{
  "allowedRegistryPrefixes": [
    "docker.io/datapower/",
    "ghcr.io/data-power-io/",
    "registry.company.com/"
  ]
}
```

### Cosign Signature Verification (Optional)

For production, you can require signed images:

```json
{
  "cosignPublicKeyPem": "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE...\n-----END PUBLIC KEY-----"
}
```

## Troubleshooting

### "Image not found" errors
- Verify image was pushed to registry
- Check registry is public or authentication is configured
- Ensure digest format is correct (`@sha256:...`)

### "Invalid catalog format" errors
- Validate JSON syntax
- Check required fields are present
- Verify image digest format

### "Registry not allowed" errors
- Add your registry to allowed prefixes
- Or remove registry restrictions

### Getting digests
If docker inspect doesn't show RepoDigests:

```bash
# Pull the image first
docker pull yourusername/noesis-postgres-connector:1.0.0

# Then inspect
docker inspect yourusername/noesis-postgres-connector:1.0.0

# Or use manifest command
docker manifest inspect yourusername/noesis-postgres-connector:1.0.0
```

## Example Complete Workflow

```bash
# 1. Build and tag image
docker build -t datapower/noesis-postgres-connector:1.0.0 ./connectors/postgres

# 2. Push to registry
docker push datapower/noesis-postgres-connector:1.0.0

# 3. Get digest
DIGEST=$(docker inspect datapower/noesis-postgres-connector:1.0.0 --format='{{index .RepoDigests 0}}')
echo "Digest: $DIGEST"

# 4. Update catalog.json with the digest
# Edit catalog/index.json and replace the image field

# 5. Commit and push
git add catalog/index.json
git commit -m "Update postgres connector digest"
git push

# 6. Add catalog source in Noesis admin:
# URL: https://github.com/data-power-io/noesis-connectors#main:catalog/index.json
```

## Next Steps

1. **Choose your registry** (Docker Hub recommended for public)
2. **Run the build script** or follow manual steps
3. **Update the catalog** with digest reference
4. **Commit to Git** and push
5. **Add catalog source** in Noesis admin dashboard

The catalog-worker will then be able to pull your connector images and make them available for installation by tenants!